// Package service 实现 gRPC 服务层：
// 请求校验、角色字段投影、交班视图组装、后台升级心跳。
package service

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "lifesupport/gen/lifesupport/v1"
	"lifesupport/internal/core"
	"lifesupport/internal/store"
)

// Server 是 LifeSupportAlerts 的实现。
type Server struct {
	pb.UnimplementedLifeSupportAlertsServer
	st  *store.Store
	now func() time.Time // 可注入时钟，重放与测试用
}

// NewServer 创建服务。now 为 nil 时使用系统时钟。
func NewServer(st *store.Store, now func() time.Time) *Server {
	if now == nil {
		now = time.Now
	}
	return &Server{st: st, now: now}
}

// Store 暴露底层存储（服务装配与测试用）。
func (s *Server) Store() *store.Store { return s.st }

// ---------------------------------------------------------------------------
// 写入类 RPC
// ---------------------------------------------------------------------------

func (s *Server) IngestSamples(ctx context.Context, req *pb.IngestSamplesRequest) (*pb.IngestSamplesResponse, error) {
	if len(req.Samples) == 0 {
		return nil, status.Error(codes.InvalidArgument, "采样批次为空")
	}
	samples := make([]core.Sample, 0, len(req.Samples))
	for i, sp := range req.Samples {
		if sp.SensorId == "" {
			return nil, status.Errorf(codes.InvalidArgument, "第 %d 条采样缺少 sensor_id", i)
		}
		if sp.DeviceTime == nil {
			return nil, status.Errorf(codes.InvalidArgument, "第 %d 条采样缺少设备采样时钟 device_time", i)
		}
		ground := sp.GroundTime
		if ground == nil {
			ground = timestamppb.New(s.now()) // 地面接收时刻缺省由服务端打时标
		}
		samples = append(samples, core.Sample{
			SensorID: sp.SensorId,
			Seq:      sp.Sequence,
			FrameID:  sp.FrameId,
			DeviceNs: sp.DeviceTime.AsTime().UnixNano(),
			GroundNs: ground.AsTime().UnixNano(),
			Value:    sp.Value,
			Unit:     sp.Unit,
			Quality:  sp.Quality,
		})
	}
	res, err := s.st.IngestSamples(samples, req.SubmissionId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "注入失败: %v", err)
	}
	return &pb.IngestSamplesResponse{
		Accepted:   int32(res.Accepted),
		Duplicates: int32(res.Duplicates),
		Conflicts:  int32(res.Conflicts),
	}, nil
}

func (s *Server) RegisterThresholdVersion(ctx context.Context, req *pb.RegisterThresholdVersionRequest) (*pb.RegisterThresholdVersionResponse, error) {
	cfg, err := core.ConfigFromProto(req.Config)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "阈值配置无效: %v", err)
	}
	if err := s.st.RegisterThresholdVersion(cfg, groundNs(req.OccurredAtGround, s.now)); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	return &pb.RegisterThresholdVersionResponse{Version: cfg.Version}, nil
}

func (s *Server) SetMissionPhase(ctx context.Context, req *pb.SetMissionPhaseRequest) (*pb.SetMissionPhaseResponse, error) {
	if req.Phase == pb.MissionPhase_MISSION_PHASE_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "任务阶段不能为空")
	}
	if req.EffectiveDeviceTime == nil {
		return nil, status.Error(codes.InvalidArgument, "必须给出阶段开始的设备时刻 effective_device_time")
	}
	err := s.st.SetMissionPhase(req.Phase, req.EffectiveDeviceTime.AsTime().UnixNano(), groundNs(req.OccurredAtGround, s.now))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "阶段切换失败: %v", err)
	}
	return &pb.SetMissionPhaseResponse{Phase: req.Phase}, nil
}

func (s *Server) AcknowledgeAlert(ctx context.Context, req *pb.AcknowledgeAlertRequest) (*pb.AcknowledgeAlertResponse, error) {
	if req.AlertId == "" || req.OperatorId == "" {
		return nil, status.Error(codes.InvalidArgument, "alert_id 与 operator_id 不能为空")
	}
	if !validRole(req.Role) {
		return nil, status.Error(codes.InvalidArgument, "role 必须为乘组/飞控/医学支持之一")
	}
	a, err := s.st.Acknowledge(req.AlertId, req.OperatorId, req.Role, req.Note, groundNs(req.OccurredAtGround, s.now))
	if err != nil {
		return nil, alertErr(err)
	}
	return &pb.AcknowledgeAlertResponse{State: a.State, CurrentOwner: a.AckBy}, nil
}

func (s *Server) AppendResolutionEvidence(ctx context.Context, req *pb.AppendResolutionEvidenceRequest) (*pb.AppendResolutionEvidenceResponse, error) {
	if req.AlertId == "" || req.OperatorId == "" {
		return nil, status.Error(codes.InvalidArgument, "alert_id 与 operator_id 不能为空")
	}
	if req.Summary == "" {
		return nil, status.Error(codes.InvalidArgument, "解除证据说明 summary 不能为空")
	}
	if !validRole(req.Role) {
		return nil, status.Error(codes.InvalidArgument, "role 必须为乘组/飞控/医学支持之一")
	}
	a, err := s.st.AppendResolution(req.AlertId, req.OperatorId, req.Role, req.Summary, groundNs(req.OccurredAtGround, s.now))
	if err != nil {
		return nil, alertErr(err)
	}
	res, err := s.st.ListResolutions(a.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.AppendResolutionEvidenceResponse{State: a.State, EvidenceCount: int32(len(res))}, nil
}

// DeleteAlert 一律拒绝：已确认的红色告警不可删除，只能追加解除证据；
// 其余告警同样为只增不改的审计记录。
func (s *Server) DeleteAlert(ctx context.Context, req *pb.DeleteAlertRequest) (*pb.DeleteAlertResponse, error) {
	if req.AlertId == "" {
		return nil, status.Error(codes.InvalidArgument, "alert_id 不能为空")
	}
	a, err := s.st.GetAlert(req.AlertId)
	if err != nil {
		return nil, alertErr(err)
	}
	if a.Severity == pb.Severity_SEVERITY_CRITICAL && a.State == pb.AlertState_ALERT_STATE_ACKNOWLEDGED {
		return nil, status.Errorf(codes.FailedPrecondition,
			"告警 %s 为已确认的红色告警，不可删除；只能追加解除证据（AppendResolutionEvidence）", req.AlertId)
	}
	return nil, status.Errorf(codes.FailedPrecondition,
		"告警 %s 为只增不改的审计记录，不允许删除；如需闭环请追加解除证据", req.AlertId)
}

// ---------------------------------------------------------------------------
// 查询类 RPC
// ---------------------------------------------------------------------------

func (s *Server) GetHandoverView(ctx context.Context, req *pb.GetHandoverViewRequest) (*pb.GetHandoverViewResponse, error) {
	if !validRole(req.Role) {
		return nil, status.Error(codes.InvalidArgument, "role 必须为乘组/飞控/医学支持之一")
	}
	nowNs := s.now().UnixNano()
	alerts, err := s.st.ListAlerts()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	resp := &pb.GetHandoverViewResponse{GeneratedAt: timestamppb.New(s.now())}
	for i := range alerts {
		full, err := s.buildHandoverAlert(&alerts[i], nowNs)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%v", err)
		}
		resp.Alerts = append(resp.Alerts, projectForRole(full, req.Role))
	}
	// 传感器健康：飞控与医学需要判断数据质量；乘组视图以告警级“证据不足”标记代替。
	if req.Role == pb.Role_ROLE_FLIGHT_CONTROL || req.Role == pb.Role_ROLE_MEDICAL {
		health, err := s.st.SensorHealth()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%v", err)
		}
		for _, h := range health {
			resp.SensorHealth = append(resp.SensorHealth, &pb.SensorHealth{
				SensorId:             h.SensorID,
				LastSequence:         h.LastSeq,
				LastDeviceTime:       nsToTs(h.LastDeviceNs),
				LastGroundTime:       nsToTs(h.LastGroundNs),
				SilenceFor:           durationpb.New(time.Duration(max64(0, nowNs-h.LastGroundNs))),
				DriftObservations:    int32(h.DriftCount),
				ConflictObservations: int32(h.ConflictCount),
			})
		}
	}
	return resp, nil
}

func (s *Server) GetAlert(ctx context.Context, req *pb.GetAlertRequest) (*pb.GetAlertResponse, error) {
	if !validRole(req.Role) {
		return nil, status.Error(codes.InvalidArgument, "role 必须为乘组/飞控/医学支持之一")
	}
	a, err := s.st.GetAlert(req.AlertId)
	if err != nil {
		return nil, alertErr(err)
	}
	full, err := s.buildHandoverAlert(a, s.now().UnixNano())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.GetAlertResponse{Alert: projectForRole(full, req.Role)}, nil
}

func (s *Server) ListEvaluations(ctx context.Context, req *pb.ListEvaluationsRequest) (*pb.ListEvaluationsResponse, error) {
	evals, err := s.st.ListEvaluations(req.Rule, int(req.Limit))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	resp := &pb.ListEvaluationsResponse{}
	for _, e := range evals {
		resp.Evaluations = append(resp.Evaluations, &pb.EvaluationRecord{
			Id:                  e.ID,
			Rule:                e.Rule,
			WindowEndDeviceTime: nsToTs(e.EndDeviceNs),
			Verdict:             e.Verdict,
			Severity:            e.Severity,
			ThresholdVersion:    e.ThresholdVersion,
			MissionPhase:        e.Phase,
			Detail:              e.Detail,
			GroundTime:          nsToTs(e.GroundNs),
		})
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// 交班视图组装与角色投影
// ---------------------------------------------------------------------------

// buildHandoverAlert 组装一条告警的完整交班字段（未投影）。
func (s *Server) buildHandoverAlert(a *store.Alert, nowNs int64) (*pb.HandoverAlert, error) {
	out := &pb.HandoverAlert{
		AlertId:                a.ID,
		Rule:                   a.Rule,
		Severity:               a.Severity,
		State:                  a.State,
		ThresholdVersion:       a.ThresholdVersion,
		MissionPhase:           a.Phase,
		InsufficientEvidence:   a.Insufficient,
		MissingCorroborators:   a.Missing,
		OpenedDeviceTime:       nsToTs(a.OpenedDeviceNs),
		OpenedGroundTime:       nsToTs(a.OpenedGroundNs),
		CurrentEscalationLevel: a.EscLevel,
	}

	// 规则文案（简述与乘组处置建议）取自告警开启时的阈值版本。
	if cfg, err := s.st.ConfigByVersion(a.ThresholdVersion); err == nil && cfg != nil {
		if rule, ok := cfg.RuleFor(a.Rule, a.Phase); ok {
			if a.Severity == pb.Severity_SEVERITY_CRITICAL {
				out.Summary = rule.SummaryCritical
				out.RecommendedCrewAction = rule.CrewActionCritical
			} else {
				out.Summary = rule.SummaryWarning
				out.RecommendedCrewAction = rule.CrewActionWarning
			}
		}
	}

	// 接管人。
	if a.State == pb.AlertState_ALERT_STATE_ACKNOWLEDGED || a.AckBy != "" {
		out.CurrentOwner = a.AckBy
		out.OwnerRole = a.AckRole
		out.AcknowledgedAt = nsToTs(a.AckGroundNs)
	}

	// 距离下一次升级还有多久。
	if a.NextEscGroundNs > 0 && a.State != pb.AlertState_ALERT_STATE_RESOLVED {
		out.NextEscalationDue = nsToTs(a.NextEscGroundNs)
		out.EscalationCountdown = durationpb.New(time.Duration(max64(0, a.NextEscGroundNs-nowNs)))
	}

	escs, err := s.st.ListEscalations(a.ID)
	if err != nil {
		return nil, err
	}
	for _, e := range escs {
		out.EscalationHistory = append(out.EscalationHistory, &pb.EscalationEntry{
			Level:              e.Level,
			DueGroundTime:      nsToTs(e.DueGroundNs),
			RecordedGroundTime: nsToTs(e.RecordedNs),
			Reason:             e.Reason,
		})
	}

	acks, err := s.st.ListAcknowledgements(a.ID)
	if err != nil {
		return nil, err
	}
	for _, ak := range acks {
		out.AcknowledgementHistory = append(out.AcknowledgementHistory, &pb.AcknowledgementEntry{
			OperatorId: ak.By, Role: ak.Role, Note: ak.Note, GroundTime: nsToTs(ak.GroundNs),
		})
	}

	res, err := s.st.ListResolutions(a.ID)
	if err != nil {
		return nil, err
	}
	for _, r := range res {
		out.ResolutionEvidence = append(out.ResolutionEvidence, &pb.ResolutionEntry{
			Summary: r.Summary, AppendedBy: r.By, Role: r.Role,
			GroundTime: nsToTs(r.GroundNs), AutoGenerated: r.Auto,
		})
	}

	// 证据采样与暴露统计。
	ev, err := s.st.AlertEvidence(a.ID)
	if err != nil {
		return nil, err
	}
	cfg, _ := s.st.ConfigByVersion(a.ThresholdVersion)
	for _, sm := range ev {
		es := &pb.EvidenceSample{
			SensorId:   sm.SensorID,
			Sequence:   sm.Seq,
			DeviceTime: nsToTs(sm.DeviceNs),
			Value:      sm.Value,
			Unit:       sm.Unit,
		}
		if cfg != nil {
			if rule, ok := cfg.RuleFor(a.Rule, a.Phase); ok {
				es.BreachedLimit = breachedLimit(rule, sm.Value)
			}
		}
		out.EvidenceSamples = append(out.EvidenceSamples, es)
	}
	if cfg != nil {
		if rule, ok := cfg.RuleFor(a.Rule, a.Phase); ok {
			maxV, meanV, warnDur, critDur := core.ExposureStats(ev, rule)
			out.Exposure = &pb.ExposureStats{
				MaxValue:              maxV,
				MeanValue:             meanV,
				DurationAboveWarning:  durationpb.New(time.Duration(warnDur)),
				DurationAboveCritical: durationpb.New(time.Duration(critDur)),
				SampleCount:           int32(len(ev)),
			}
		}
	}
	return out, nil
}

// projectForRole 按角色裁剪字段：各角色只取得履职所需字段。
func projectForRole(full *pb.HandoverAlert, role pb.Role) *pb.HandoverAlert {
	if role == pb.Role_ROLE_FLIGHT_CONTROL {
		return full // 飞控取得全部字段
	}
	a := proto.Clone(full).(*pb.HandoverAlert) // 深拷贝后按需清空
	switch role {
	case pb.Role_ROLE_CREW:
		// 乘组：可执行摘要、当前接管人、证据是否充分、解除进展。
		a.EscalationHistory = nil
		a.EvidenceSamples = nil
		a.Exposure = nil
		a.ThresholdVersion = ""
		a.NextEscalationDue = nil
		a.EscalationCountdown = nil
		a.CurrentEscalationLevel = 0
		a.AcknowledgementHistory = nil
	case pb.Role_ROLE_MEDICAL:
		// 医学支持：暴露统计、证据取值、解除证据链；不展示运行升级路由。
		a.EscalationHistory = nil
		a.NextEscalationDue = nil
		a.EscalationCountdown = nil
		a.CurrentEscalationLevel = 0
		a.ThresholdVersion = ""
		a.AcknowledgementHistory = nil
	}
	return a
}

func breachedLimit(rule core.ThresholdRule, value float64) float64 {
	if rule.Breaches(value, pb.Severity_SEVERITY_CRITICAL) {
		return rule.LimitFor(pb.Severity_SEVERITY_CRITICAL)
	}
	if rule.Breaches(value, pb.Severity_SEVERITY_WARNING) {
		return rule.LimitFor(pb.Severity_SEVERITY_WARNING)
	}
	return 0
}

// ---------------------------------------------------------------------------
// 后台心跳：推进地面时钟，触发到期升级
// ---------------------------------------------------------------------------

// StartHeartbeat 以固定间隔注入心跳事件（地面时钟推进），直到返回的停止函数被调用。
func (s *Server) StartHeartbeat(interval time.Duration) (stop func()) {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				_ = s.st.Heartbeat(s.now().UnixNano())
			}
		}
	}()
	return func() { close(done) }
}

// Tick 供测试以指定地面时刻触发一次心跳。
func (s *Server) Tick(groundNs int64) error { return s.st.Heartbeat(groundNs) }

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

func validRole(r pb.Role) bool {
	return r == pb.Role_ROLE_CREW || r == pb.Role_ROLE_FLIGHT_CONTROL || r == pb.Role_ROLE_MEDICAL
}

func groundNs(ts *timestamppb.Timestamp, now func() time.Time) int64 {
	if ts != nil {
		return ts.AsTime().UnixNano()
	}
	return now().UnixNano()
}

func nsToTs(ns int64) *timestamppb.Timestamp {
	if ns == 0 {
		return nil
	}
	return timestamppb.New(time.Unix(0, ns).UTC())
}

func alertErr(err error) error {
	if err == nil {
		return nil
	}
	if isNotFound(err) {
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.FailedPrecondition, err.Error())
}

func isNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "不存在")
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
