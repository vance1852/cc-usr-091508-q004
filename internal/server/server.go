// Package server 提供生命保障告警的 gRPC 服务层。
// 所有状态变更经由 engine.Apply 进入事件日志; 查询按角色裁剪字段。
package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	lsv1 "lifesupport/gen/lifesupport/v1"
	"lifesupport/internal/engine"
)

// Server 实现 lsv1.LifeSupportServer。
type Server struct {
	lsv1.UnimplementedLifeSupportServer
	eng *engine.Engine
	now func() int64
}

func New(eng *engine.Engine, now func() int64) *Server {
	return &Server{eng: eng, now: now}
}

func toErr(err error) error {
	if err == nil {
		return nil
	}
	return status.Error(codes.FailedPrecondition, err.Error())
}

func (s *Server) IngestTelemetry(ctx context.Context, req *lsv1.IngestTelemetryRequest) (*lsv1.IngestTelemetryResponse, error) {
	res, err := s.eng.Apply(engine.EvSampleBatch, &lsv1.SampleBatchEvent{Samples: req.Samples},
		uid("ingest", req.IdempotencyKey), s.now())
	if err != nil {
		return nil, toErr(err)
	}
	return &lsv1.IngestTelemetryResponse{
		EventUid:         res.EventUID,
		DuplicateRequest: res.DuplicateRequest,
		Accepted:         res.Accepted,
		Duplicates:       res.Duplicates,
		NewAlertIds:      res.NewAlertIDs,
	}, nil
}

func (s *Server) ActivateThresholds(ctx context.Context, req *lsv1.ActivateThresholdsRequest) (*lsv1.ActivateThresholdsResponse, error) {
	op := operator(req.Meta)
	res, err := s.eng.Apply(engine.EvThresholds, &lsv1.ThresholdsActivatedEvent{
		Version:               req.Version,
		EffectiveFromDeviceNs: req.EffectiveFromDeviceNs,
		Rules:                 req.Rules,
		ActivatedBy:           op,
		Note:                  req.Note,
	}, uid("thresh", req.IdempotencyKey), s.now())
	if err != nil {
		return nil, toErr(err)
	}
	return &lsv1.ActivateThresholdsResponse{EventUid: res.EventUID, DuplicateRequest: res.DuplicateRequest}, nil
}

func (s *Server) SetMissionPhase(ctx context.Context, req *lsv1.SetMissionPhaseRequest) (*lsv1.SetMissionPhaseResponse, error) {
	res, err := s.eng.Apply(engine.EvPhase, &lsv1.PhaseChangedEvent{
		Phase:                 req.Phase,
		EffectiveFromDeviceNs: req.EffectiveFromDeviceNs,
		SetBy:                 operator(req.Meta),
	}, uid("phase", req.IdempotencyKey), s.now())
	if err != nil {
		return nil, toErr(err)
	}
	return &lsv1.SetMissionPhaseResponse{EventUid: res.EventUID, DuplicateRequest: res.DuplicateRequest}, nil
}

func (s *Server) AckAlert(ctx context.Context, req *lsv1.AckAlertRequest) (*lsv1.AlertResponse, error) {
	meta := req.Meta
	res, err := s.eng.Apply(engine.EvAck, &lsv1.AckEvent{
		AlertId:  req.AlertId,
		Operator: operator(meta),
		Role:     roleOf(meta),
		Note:     req.Note,
	}, uid("ack", req.IdempotencyKey), s.now())
	if err != nil {
		return nil, toErr(err)
	}
	return s.alertReply(req.AlertId, roleOf(meta), res)
}

func (s *Server) AssignAlert(ctx context.Context, req *lsv1.AssignAlertRequest) (*lsv1.AlertResponse, error) {
	res, err := s.eng.Apply(engine.EvAssign, &lsv1.AssignEvent{
		AlertId:  req.AlertId,
		Operator: req.Assignee,
		Role:     req.AssigneeRole,
		Note:     req.Note,
	}, uid("assign", req.IdempotencyKey), s.now())
	if err != nil {
		return nil, toErr(err)
	}
	return s.alertReply(req.AlertId, roleOf(req.Meta), res)
}

func (s *Server) AppendEvidence(ctx context.Context, req *lsv1.AppendEvidenceRequest) (*lsv1.AlertResponse, error) {
	res, err := s.eng.Apply(engine.EvEvidence, &lsv1.EvidenceEvent{
		AlertId:  req.AlertId,
		Operator: operator(req.Meta),
		Summary:  req.Summary,
		Detail:   req.Detail,
	}, uid("evidence", req.IdempotencyKey), s.now())
	if err != nil {
		return nil, toErr(err)
	}
	return s.alertReply(req.AlertId, roleOf(req.Meta), res)
}

func (s *Server) ResolveAlert(ctx context.Context, req *lsv1.ResolveAlertRequest) (*lsv1.AlertResponse, error) {
	res, err := s.eng.Apply(engine.EvResolve, &lsv1.ResolveEvent{
		AlertId:  req.AlertId,
		Operator: operator(req.Meta),
		Summary:  req.Summary,
		Detail:   req.Detail,
	}, uid("resolve", req.IdempotencyKey), s.now())
	if err != nil {
		return nil, toErr(err)
	}
	return s.alertReply(req.AlertId, roleOf(req.Meta), res)
}

func (s *Server) GetAlert(ctx context.Context, req *lsv1.GetAlertRequest) (*lsv1.AlertResponse, error) {
	v := s.eng.AlertViewByID(req.AlertId, s.now())
	if v == nil {
		return nil, status.Errorf(codes.NotFound, "alert %q not found", req.AlertId)
	}
	return &lsv1.AlertResponse{Alert: MaskAlertFor(roleOf(req.Meta), v)}, nil
}

func (s *Server) ListAlerts(ctx context.Context, req *lsv1.ListAlertsRequest) (*lsv1.ListAlertsResponse, error) {
	views := s.eng.ListAlertViews(req.IncludeResolved, s.now())
	return &lsv1.ListAlertsResponse{Alerts: MaskAlertListFor(roleOf(req.Meta), views)}, nil
}

func (s *Server) GetHandoverView(ctx context.Context, req *lsv1.GetHandoverViewRequest) (*lsv1.HandoverView, error) {
	return MaskHandoverFor(roleOf(req.Meta), s.eng.HandoverView(s.now())), nil
}

func (s *Server) GetEnvironmentStatus(ctx context.Context, req *lsv1.GetEnvironmentStatusRequest) (*lsv1.EnvironmentStatusResponse, error) {
	return s.eng.EnvironmentStatus(s.now()), nil
}

func (s *Server) alertReply(alertID string, role lsv1.Role, res *engine.ApplyResult) (*lsv1.AlertResponse, error) {
	v := s.eng.AlertViewByID(alertID, s.now())
	if v == nil {
		return nil, status.Errorf(codes.NotFound, "alert %q not found", alertID)
	}
	return &lsv1.AlertResponse{
		EventUid:         res.EventUID,
		DuplicateRequest: res.DuplicateRequest,
		Alert:            MaskAlertFor(role, v),
	}, nil
}

func uid(prefix, key string) string {
	if key == "" {
		return "" // 引擎按内容哈希派生幂等键
	}
	return prefix + ":" + key
}

func operator(m *lsv1.RequestMeta) string {
	if m == nil {
		return ""
	}
	return m.Operator
}

func roleOf(m *lsv1.RequestMeta) lsv1.Role {
	if m == nil || m.Role == lsv1.Role_ROLE_UNSPECIFIED {
		return lsv1.Role_ROLE_CREW // 未声明角色时按最保守视图
	}
	return m.Role
}
