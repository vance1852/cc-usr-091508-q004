package service

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "lifesupport/gen/lifesupport/v1"
	"lifesupport/internal/core"
	"lifesupport/internal/store"
)

const sec = int64(time.Second)
const T0 = int64(1_700_000_000_000_000_000)

// newTestClient 启动 bufconn 内存 gRPC 服务并返回客户端与可控时钟。
func newTestClient(t *testing.T) (pb.LifeSupportAlertsClient, *Server, *int64) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "svc.db"))
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	clock := new(int64)
	*clock = T0 // 测试时钟从 T0 起，可随场景推进
	srv := NewServer(st, func() time.Time { return time.Unix(0, *clock).UTC() })

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterLifeSupportAlertsServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewLifeSupportAlertsClient(conn), srv, clock
}

func ts(ns int64) *timestamppb.Timestamp { return timestamppb.New(time.Unix(0, ns).UTC()) }

func sampleMsg(sensor string, seq int64, devNs, groundNs int64, val float64) *pb.Sample {
	return &pb.Sample{
		SensorId:   sensor,
		Sequence:   seq,
		FrameId:    frameID(sensor, seq),
		DeviceTime: ts(devNs),
		GroundTime: ts(groundNs),
		Value:      val,
	}
}

// co2Samples 生成一对 CO2 通道的采样消息。
func co2Samples(seqStart int, devStart int64, n int, pv, sv float64) []*pb.Sample {
	var out []*pb.Sample
	for i := 0; i < n; i++ {
		dev := devStart + int64(seqStart-1+i)*10*sec
		out = append(out,
			sampleMsg("co2_primary", int64(seqStart+i), dev, dev+5*sec, pv),
			sampleMsg("co2_secondary", int64(seqStart+i), dev, dev+7*sec, sv),
		)
	}
	return out
}

func frameID(sensor string, seq int64) string {
	return fmt.Sprintf("%s#%d", sensor, seq)
}

func setupScenario(t *testing.T, cli pb.LifeSupportAlertsClient) {
	t.Helper()
	ctx := context.Background()
	// 通过 RPC 注册默认阈值版本（proto 往返）。
	cfg := defaultConfigProto()
	if _, err := cli.RegisterThresholdVersion(ctx, &pb.RegisterThresholdVersionRequest{
		Config: cfg, OccurredAtGround: ts(T0),
	}); err != nil {
		t.Fatalf("注册阈值版本失败: %v", err)
	}
	if _, err := cli.SetMissionPhase(ctx, &pb.SetMissionPhaseRequest{
		Phase: pb.MissionPhase_MISSION_PHASE_ON_ORBIT, EffectiveDeviceTime: ts(0), OccurredAtGround: ts(T0),
	}); err != nil {
		t.Fatalf("设置阶段失败: %v", err)
	}
}

func defaultConfigProto() *pb.ThresholdVersionConfig {
	d := core.DefaultThresholdVersion()
	cfg := &pb.ThresholdVersionConfig{Version: d.Version, EffectiveFromDeviceTime: ts(d.EffectiveDeviceNs)}
	mkDur := func(ns int64) *durationpb.Duration { return durationpb.New(time.Duration(ns)) }
	for _, r := range d.Rules {
		cfg.Rules = append(cfg.Rules, &pb.ThresholdRule{
			Name:                  r.Name,
			MetricSensor:          r.MetricSensor,
			Unit:                  r.Unit,
			Comparison:            r.Comparison,
			WarningLimit:          r.WarningLimit,
			CriticalLimit:         r.CriticalLimit,
			WarningWindow:         mkDur(r.WarningWindowNs),
			CriticalWindow:        mkDur(r.CriticalWindowNs),
			RecoveryWindow:        mkDur(r.RecoveryWindowNs),
			MaxSampleGap:          mkDur(r.MaxSampleGapNs),
			Corroborators:         r.Corroborators,
			CorroboratorStaleness: mkDur(r.CorroboratorStaleNs),
			SummaryWarning:        r.SummaryWarning,
			SummaryCritical:       r.SummaryCritical,
			CrewActionWarning:     r.CrewActionWarning,
			CrewActionCritical:    r.CrewActionCritical,
		})
	}
	mkSteps := func(steps []core.EscalationStep) []*pb.EscalationStep {
		var out []*pb.EscalationStep
		for _, s := range steps {
			out = append(out, &pb.EscalationStep{
				Level:               s.Level,
				AfterUnacknowledged: mkDur(s.AfterUnackNs),
				AfterUnresolved:     mkDur(s.AfterUnresolvedNs),
				Notify:              s.Notify,
			})
		}
		return out
	}
	cfg.Escalation = &pb.EscalationPolicy{
		CriticalSteps: mkSteps(d.Policy.Critical),
		WarningSteps:  mkSteps(d.Policy.Warning),
	}
	for _, ov := range d.Overrides {
		po := &pb.PhaseLimitOverride{Phase: ov.Phase, Rule: ov.Rule}
		if ov.WarnLimit != nil {
			po.WarningLimit = ov.WarnLimit
		}
		if ov.CritLimit != nil {
			po.CriticalLimit = ov.CritLimit
		}
		cfg.PhaseOverrides = append(cfg.PhaseOverrides, po)
	}
	return cfg
}

// openCO2Alert 注入持续越限采样形成红警，返回告警 ID。
func openCO2Alert(t *testing.T, cli pb.LifeSupportAlertsClient) string {
	t.Helper()
	ctx := context.Background()
	res, err := cli.IngestSamples(ctx, &pb.IngestSamplesRequest{
		Samples: co2Samples(1, T0, 40, 5.5, 5.4), SubmissionId: "open-1",
	})
	if err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	if res.Accepted != 80 {
		t.Fatalf("应接受 80 条采样: %+v", res)
	}
	view, err := cli.GetHandoverView(ctx, &pb.GetHandoverViewRequest{Role: pb.Role_ROLE_FLIGHT_CONTROL})
	if err != nil {
		t.Fatalf("交班视图失败: %v", err)
	}
	if len(view.Alerts) != 1 {
		t.Fatalf("应形成 1 条告警，得到 %d", len(view.Alerts))
	}
	return view.Alerts[0].AlertId
}

func TestEndToEndAlertLifecycle(t *testing.T) {
	cli, srv, clock := newTestClient(t)
	ctx := context.Background()
	setupScenario(t, cli)
	id := openCO2Alert(t, cli)
	*clock = T0 + 310*sec // 推进测试时钟到告警开启后不久

	// 飞控视图：完整字段。
	view, err := cli.GetHandoverView(ctx, &pb.GetHandoverViewRequest{Role: pb.Role_ROLE_FLIGHT_CONTROL})
	if err != nil {
		t.Fatalf("交班视图失败: %v", err)
	}
	a := view.Alerts[0]
	if a.Severity != pb.Severity_SEVERITY_CRITICAL || a.State != pb.AlertState_ALERT_STATE_OPEN {
		t.Fatalf("告警状态错误: %v/%v", a.Severity, a.State)
	}
	if len(a.EvidenceSamples) != 40 {
		t.Fatalf("飞控应看到 40 条证据采样，得到 %d", len(a.EvidenceSamples))
	}
	if a.ThresholdVersion != "v1-on-orbit" {
		t.Fatalf("飞控应看到阈值版本: %s", a.ThresholdVersion)
	}
	if a.Summary == "" || a.RecommendedCrewAction == "" {
		t.Fatalf("应有简述与处置建议")
	}
	// 距离下一次升级还有多久：未确认红警 2 分钟。
	if a.NextEscalationDue == nil || a.EscalationCountdown == nil {
		t.Fatalf("应给出下一次升级时刻与倒计时")
	}
	wantDue := T0 + 305*sec + 2*60*sec
	if a.NextEscalationDue.AsTime().UnixNano() != wantDue {
		t.Fatalf("下一升级时刻应为 %d，得到 %d", wantDue, a.NextEscalationDue.AsTime().UnixNano())
	}
	if a.EscalationCountdown.AsDuration() != 115*time.Second {
		t.Fatalf("倒计时应为 115s（开启后 2 分钟减去已流逝），得到 %v", a.EscalationCountdown.AsDuration())
	}
	// 传感器健康对飞控可见。
	if len(view.SensorHealth) != 2 {
		t.Fatalf("飞控应看到 2 个传感器健康项，得到 %d", len(view.SensorHealth))
	}

	// 确认接管。
	ack, err := cli.AcknowledgeAlert(ctx, &pb.AcknowledgeAlertRequest{
		AlertId: id, OperatorId: "li-wei", Role: pb.Role_ROLE_FLIGHT_CONTROL,
		Note: "值班接管", OccurredAtGround: ts(T0 + 400*sec),
	})
	if err != nil {
		t.Fatalf("确认失败: %v", err)
	}
	if ack.CurrentOwner != "li-wei" || ack.State != pb.AlertState_ALERT_STATE_ACKNOWLEDGED {
		t.Fatalf("确认结果错误: %+v", ack)
	}

	// 确认后升级时刻表切换：下一次升级为未解除督办（确认后 30 分钟）。
	got, err := cli.GetAlert(ctx, &pb.GetAlertRequest{AlertId: id, Role: pb.Role_ROLE_FLIGHT_CONTROL})
	if err != nil {
		t.Fatalf("查询告警失败: %v", err)
	}
	wantDue = T0 + 400*sec + 30*60*sec
	if got.Alert.NextEscalationDue.AsTime().UnixNano() != wantDue {
		t.Fatalf("确认后下一升级应为 %d，得到 %d", wantDue, got.Alert.NextEscalationDue.AsTime().UnixNano())
	}
	if got.Alert.CurrentOwner != "li-wei" || got.Alert.OwnerRole != pb.Role_ROLE_FLIGHT_CONTROL {
		t.Fatalf("接管人字段错误: %+v", got.Alert)
	}

	// 心跳推进到确认后 31 分钟：触发未解除督办升级。
	if err := srv.Tick(T0 + 400*sec + 31*60*sec); err != nil {
		t.Fatalf("心跳失败: %v", err)
	}
	got, _ = cli.GetAlert(ctx, &pb.GetAlertRequest{AlertId: id, Role: pb.Role_ROLE_FLIGHT_CONTROL})
	if got.Alert.CurrentEscalationLevel != 4 {
		t.Fatalf("应升级到 L4，得到 L%d", got.Alert.CurrentEscalationLevel)
	}
	if len(got.Alert.EscalationHistory) != 1 || got.Alert.EscalationHistory[0].Level != 4 {
		t.Fatalf("升级历史应含 L4: %+v", got.Alert.EscalationHistory)
	}
}

func TestRoleProjections(t *testing.T) {
	cli, _, _ := newTestClient(t)
	ctx := context.Background()
	setupScenario(t, cli)
	id := openCO2Alert(t, cli)
	if _, err := cli.AcknowledgeAlert(ctx, &pb.AcknowledgeAlertRequest{
		AlertId: id, OperatorId: "li-wei", Role: pb.Role_ROLE_FLIGHT_CONTROL, OccurredAtGround: ts(T0 + 400*sec),
	}); err != nil {
		t.Fatalf("确认失败: %v", err)
	}

	// 乘组：履职所需字段——简述、处置建议、接管人；无原始证据与升级细节。
	crew, err := cli.GetHandoverView(ctx, &pb.GetHandoverViewRequest{Role: pb.Role_ROLE_CREW})
	if err != nil {
		t.Fatalf("乘组视图失败: %v", err)
	}
	ca := crew.Alerts[0]
	if ca.Summary == "" || ca.RecommendedCrewAction == "" || ca.CurrentOwner != "li-wei" {
		t.Fatalf("乘组视图缺少履职字段: %+v", ca)
	}
	if len(ca.EvidenceSamples) != 0 || ca.ThresholdVersion != "" || ca.NextEscalationDue != nil ||
		len(ca.EscalationHistory) != 0 || ca.Exposure != nil {
		t.Fatalf("乘组视图不应包含证据采样/阈值版本/升级细节: %+v", ca)
	}
	if len(crew.SensorHealth) != 0 {
		t.Fatalf("乘组视图不含传感器健康明细")
	}

	// 医学：暴露统计与证据取值可见，无升级路由。
	med, err := cli.GetHandoverView(ctx, &pb.GetHandoverViewRequest{Role: pb.Role_ROLE_MEDICAL})
	if err != nil {
		t.Fatalf("医学视图失败: %v", err)
	}
	ma := med.Alerts[0]
	if ma.Exposure == nil || ma.Exposure.MaxValue != 5.5 || ma.Exposure.SampleCount != 40 {
		t.Fatalf("医学视图应有暴露统计: %+v", ma.Exposure)
	}
	if ma.Exposure.DurationAboveCritical.AsDuration() <= 0 {
		t.Fatalf("应有越红限暴露时长")
	}
	if len(ma.EvidenceSamples) != 40 {
		t.Fatalf("医学视图应有证据采样取值")
	}
	if ma.NextEscalationDue != nil || len(ma.EscalationHistory) != 0 || ma.ThresholdVersion != "" {
		t.Fatalf("医学视图不应包含升级路由/阈值版本: %+v", ma)
	}
	if len(med.SensorHealth) != 2 {
		t.Fatalf("医学视图应包含传感器健康（数据质量判断）")
	}

	// 飞控：全字段。
	fcr, err := cli.GetHandoverView(ctx, &pb.GetHandoverViewRequest{Role: pb.Role_ROLE_FLIGHT_CONTROL})
	if err != nil {
		t.Fatalf("飞控视图失败: %v", err)
	}
	fa := fcr.Alerts[0]
	if fa.ThresholdVersion == "" || fa.NextEscalationDue == nil || fa.Exposure == nil ||
		len(fa.EvidenceSamples) != 40 || len(fa.AcknowledgementHistory) != 1 {
		t.Fatalf("飞控视图应包含全部字段: %+v", fa)
	}

	// 未指定角色：拒绝。
	if _, err := cli.GetHandoverView(ctx, &pb.GetHandoverViewRequest{Role: pb.Role_ROLE_UNSPECIFIED}); err == nil {
		t.Fatalf("未指定角色应拒绝")
	}
}

func TestDeleteAlertForbidden(t *testing.T) {
	cli, _, _ := newTestClient(t)
	ctx := context.Background()
	setupScenario(t, cli)
	id := openCO2Alert(t, cli)
	if _, err := cli.AcknowledgeAlert(ctx, &pb.AcknowledgeAlertRequest{
		AlertId: id, OperatorId: "li-wei", Role: pb.Role_ROLE_FLIGHT_CONTROL, OccurredAtGround: ts(T0 + 400*sec),
	}); err != nil {
		t.Fatalf("确认失败: %v", err)
	}

	// 已确认的红色告警：明确拒绝删除。
	_, err := cli.DeleteAlert(ctx, &pb.DeleteAlertRequest{AlertId: id, OperatorId: "li-wei", Role: pb.Role_ROLE_FLIGHT_CONTROL})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("已确认红色告警删除应返回 FailedPrecondition，得到 %v", err)
	}

	// 只能追加解除证据。
	res, err := cli.AppendResolutionEvidence(ctx, &pb.AppendResolutionEvidenceRequest{
		AlertId: id, OperatorId: "li-wei", Role: pb.Role_ROLE_FLIGHT_CONTROL,
		Summary: "备用清除装置投运，CO2 回落至 2.0", OccurredAtGround: ts(T0 + 600*sec),
	})
	if err != nil {
		t.Fatalf("追加解除证据失败: %v", err)
	}
	if res.State != pb.AlertState_ALERT_STATE_RESOLVED || res.EvidenceCount != 1 {
		t.Fatalf("解除结果错误: %+v", res)
	}

	// 解除后依然不可删除（审计记录只增不改）。
	if _, err := cli.DeleteAlert(ctx, &pb.DeleteAlertRequest{AlertId: id, OperatorId: "li-wei", Role: pb.Role_ROLE_FLIGHT_CONTROL}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("已解除告警同样不可删除，得到 %v", err)
	}
	// 告警仍可查询，证据链完整。
	got, err := cli.GetAlert(ctx, &pb.GetAlertRequest{AlertId: id, Role: pb.Role_ROLE_FLIGHT_CONTROL})
	if err != nil {
		t.Fatalf("解除后告警应可查询: %v", err)
	}
	if len(got.Alert.ResolutionEvidence) != 1 || got.Alert.ResolutionEvidence[0].AppendedBy != "li-wei" {
		t.Fatalf("解除证据链应保留: %+v", got.Alert.ResolutionEvidence)
	}
	// 不存在的告警：NotFound。
	if _, err := cli.DeleteAlert(ctx, &pb.DeleteAlertRequest{AlertId: "no-such", OperatorId: "x", Role: pb.Role_ROLE_FLIGHT_CONTROL}); status.Code(err) != codes.NotFound {
		t.Fatalf("不存在的告警应返回 NotFound，得到 %v", err)
	}
}

func TestListEvaluationsAudit(t *testing.T) {
	cli, _, _ := newTestClient(t)
	ctx := context.Background()
	setupScenario(t, cli)
	openCO2Alert(t, cli)
	resp, err := cli.ListEvaluations(ctx, &pb.ListEvaluationsRequest{Rule: "co2_high", Limit: 500})
	if err != nil {
		t.Fatalf("查询判定失败: %v", err)
	}
	if len(resp.Evaluations) == 0 {
		t.Fatalf("应有判定记录")
	}
	var sustained int
	for _, e := range resp.Evaluations {
		if e.ThresholdVersion != "v1-on-orbit" || e.MissionPhase != pb.MissionPhase_MISSION_PHASE_ON_ORBIT {
			t.Fatalf("判定应记录阈值版本与任务阶段: %+v", e)
		}
		if e.Verdict == pb.Verdict_VERDICT_SUSTAINED_BREACH {
			sustained++
		}
	}
	if sustained == 0 {
		t.Fatalf("应存在持续越限判定")
	}
}

func TestIngestValidation(t *testing.T) {
	cli, _, _ := newTestClient(t)
	ctx := context.Background()
	if _, err := cli.IngestSamples(ctx, &pb.IngestSamplesRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("空批次应拒绝: %v", err)
	}
	s := sampleMsg("co2_primary", 1, T0, T0+5*sec, 2.0)
	s.DeviceTime = nil
	if _, err := cli.IngestSamples(ctx, &pb.IngestSamplesRequest{Samples: []*pb.Sample{s}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("缺少设备采样时钟应拒绝: %v", err)
	}
}
