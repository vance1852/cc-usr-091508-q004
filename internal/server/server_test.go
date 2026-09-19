package server

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	lsv1 "lifesupport/gen/lifesupport/v1"
	"lifesupport/internal/engine"
)

const sec = int64(time.Second)

var D0 = int64(1_700_000_000) * sec

type testServer struct {
	client lsv1.LifeSupportClient
	eng    *engine.Engine
	clk    *int64
	lis    *bufconn.Listener
	gs     *grpc.Server
	db     string
}

func (ts *testServer) close() {
	ts.gs.Stop()
	ts.eng.Close()
}

// start 在 db 上启动 (或重启) 服务, 模拟一次进程生命周期。
func start(t *testing.T, db string, clk *int64) *testServer {
	t.Helper()
	now := func() int64 { return *clk }
	eng, err := engine.Open(db, engine.DefaultConfig(now))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	lsv1.RegisterLifeSupportServer(gs, New(eng, now))
	go gs.Serve(lis)
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &testServer{
		client: lsv1.NewLifeSupportClient(conn),
		eng:    eng, clk: clk, lis: lis, gs: gs, db: db,
	}
}

func meta(role lsv1.Role, op string) *lsv1.RequestMeta {
	return &lsv1.RequestMeta{Role: role, Operator: op}
}

func seedViaRPC(t *testing.T, c lsv1.LifeSupportClient) {
	t.Helper()
	if _, err := c.ActivateThresholds(context.Background(), &lsv1.ActivateThresholdsRequest{
		Meta:                  meta(lsv1.Role_ROLE_FLIGHT_CONTROL, "fco-seed"),
		IdempotencyKey:        "seed:thr",
		Version:               "v1-onorbit",
		EffectiveFromDeviceNs: 0,
		Rules:                 engine.DefaultThresholds(0).Rules,
	}); err != nil {
		t.Fatalf("seed thresholds: %v", err)
	}
	if _, err := c.SetMissionPhase(context.Background(), &lsv1.SetMissionPhaseRequest{
		Meta:                  meta(lsv1.Role_ROLE_FLIGHT_CONTROL, "fco-seed"),
		IdempotencyKey:        "seed:phase",
		Phase:                 lsv1.MissionPhase_MISSION_PHASE_ON_ORBIT,
		EffectiveFromDeviceNs: 0,
	}); err != nil {
		t.Fatalf("seed phase: %v", err)
	}
}

// ingestCO2 送入 CO2 从 offSec 起 count 条 1Hz 采样。
func ingestCO2(t *testing.T, c lsv1.LifeSupportClient, key string, offSec, count int64, v float64) *lsv1.IngestTelemetryResponse {
	t.Helper()
	var ss []*lsv1.Sample
	for i := int64(0); i < count; i++ {
		dev := D0 + (offSec+i)*sec
		ss = append(ss, &lsv1.Sample{
			SensorId: "co2_pp", Seq: offSec + i, DeviceTimeNs: dev, GroundRecvNs: dev + 2*sec, Value: v,
		})
	}
	res, err := c.IngestTelemetry(context.Background(), &lsv1.IngestTelemetryRequest{
		Meta:           meta(lsv1.Role_ROLE_FLIGHT_CONTROL, "gw"),
		IdempotencyKey: key,
		Samples:        ss,
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return res
}

func TestGRPCAlertFlowAndRoleMasking(t *testing.T) {
	clk := D0 + 100*sec
	ts := start(t, ":memory:", &clk)
	defer ts.close()
	c := ts.client
	ctx := context.Background()
	seedViaRPC(t, c)

	// 61s 持续超限 => 红色告警。
	res := ingestCO2(t, c, "b1", 0, 61, 4.5)
	if len(res.NewAlertIds) != 1 {
		t.Fatalf("new alerts = %v", res.NewAlertIds)
	}
	id := res.NewAlertIds[0]

	// 飞控确认。
	if _, err := c.AckAlert(ctx, &lsv1.AckAlertRequest{
		Meta: meta(lsv1.Role_ROLE_FLIGHT_CONTROL, "fco-zhang"), AlertId: id, Note: "已通知乘组",
	}); err != nil {
		t.Fatalf("ack: %v", err)
	}
	// 交班接管。
	if _, err := c.AssignAlert(ctx, &lsv1.AssignAlertRequest{
		Meta:    meta(lsv1.Role_ROLE_FLIGHT_CONTROL, "fco-zhang"),
		AlertId: id, Assignee: "fco-li", AssigneeRole: lsv1.Role_ROLE_FLIGHT_CONTROL,
	}); err != nil {
		t.Fatalf("assign: %v", err)
	}

	// 飞控视图: 全量字段。
	fco := meta(lsv1.Role_ROLE_FLIGHT_CONTROL, "fco-li")
	hvFCO, err := c.GetHandoverView(ctx, &lsv1.GetHandoverViewRequest{Meta: fco})
	if err != nil {
		t.Fatalf("handover fco: %v", err)
	}
	if len(hvFCO.Alerts) != 1 {
		t.Fatalf("fco alerts = %d", len(hvFCO.Alerts))
	}
	a := hvFCO.Alerts[0]
	if a.ThresholdVersion == "" || len(a.ContributingSamples) != 61 || a.Owner != "fco-li" || a.AckedBy != "fco-zhang" {
		t.Fatalf("fco view incomplete: version=%s samples=%d owner=%s",
			a.ThresholdVersion, len(a.ContributingSamples), a.Owner)
	}
	if a.OwnerSinceNs == 0 || a.EscalationLevel != 1 {
		t.Fatalf("fco view: owner_since=%d level=%d", a.OwnerSinceNs, a.EscalationLevel)
	}

	// 乘组视图: 无采样明细/阈值版本/人员姓名, 但有处置程序。
	hvCrew, err := c.GetHandoverView(ctx, &lsv1.GetHandoverViewRequest{Meta: meta(lsv1.Role_ROLE_CREW, "crew-1")})
	if err != nil {
		t.Fatalf("handover crew: %v", err)
	}
	ca := hvCrew.Alerts[0]
	if len(ca.ContributingSamples) != 0 || ca.ContributingSampleCount != 61 {
		t.Fatalf("crew must not see raw samples: %d/%d", len(ca.ContributingSamples), ca.ContributingSampleCount)
	}
	if ca.ThresholdVersion != "" || ca.Owner != "" || ca.AckedBy != "" {
		t.Fatalf("crew view leaked internals: %+v", ca)
	}
	if ca.CrewProcedure == "" || ca.OwnerRole != lsv1.Role_ROLE_FLIGHT_CONTROL {
		t.Fatalf("crew view missing procedure/owner role: %+v", ca)
	}

	// 医学视图: 有暴露明细与峰值, 无升级链路与人员姓名。
	hvMed, err := c.GetHandoverView(ctx, &lsv1.GetHandoverViewRequest{Meta: meta(lsv1.Role_ROLE_MEDICAL, "med-wang")})
	if err != nil {
		t.Fatalf("handover med: %v", err)
	}
	ma := hvMed.Alerts[0]
	if len(ma.ContributingSamples) != 61 || ma.PeakValue != 4.5 {
		t.Fatalf("medical needs exposure data: samples=%d peak=%v", len(ma.ContributingSamples), ma.PeakValue)
	}
	if len(ma.EscalationHistory) != 0 || ma.Owner != "" || ma.ThresholdVersion != "" {
		t.Fatalf("medical view leaked ops fields: %+v", ma)
	}

	// 未声明角色 => 按最保守 (乘组) 裁剪。
	hvAnon, err := c.GetHandoverView(ctx, &lsv1.GetHandoverViewRequest{})
	if err != nil {
		t.Fatalf("handover anon: %v", err)
	}
	if hvAnon.Alerts[0].ThresholdVersion != "" || len(hvAnon.Alerts[0].ContributingSamples) != 0 {
		t.Fatal("anonymous role must get the most conservative view")
	}
}

func TestGRPCIdempotentIngest(t *testing.T) {
	clk := D0 + 100*sec
	ts := start(t, ":memory:", &clk)
	defer ts.close()
	seedViaRPC(t, ts.client)

	r1 := ingestCO2(t, ts.client, "batch-x", 0, 10, 1.5)
	if r1.Accepted != 10 || r1.DuplicateRequest {
		t.Fatalf("first: %+v", r1)
	}
	r2 := ingestCO2(t, ts.client, "batch-x", 0, 10, 1.5)
	if !r2.DuplicateRequest || r2.Accepted != 0 {
		t.Fatalf("retry must be a no-op: %+v", r2)
	}
}

func TestGRPCRestartRebuildsState(t *testing.T) {
	db := filepath.Join(t.TempDir(), "events.db")
	clk := D0 + 100*sec

	ts := start(t, db, &clk)
	seedViaRPC(t, ts.client)
	ingestCO2(t, ts.client, "b1", 0, 61, 4.5)
	ctx := context.Background()
	if _, err := ts.client.AckAlert(ctx, &lsv1.AckAlertRequest{
		Meta: meta(lsv1.Role_ROLE_FLIGHT_CONTROL, "fco-zhang"), AlertId: "ALR-0001",
	}); err != nil {
		t.Fatalf("ack: %v", err)
	}
	before, err := ts.client.GetHandoverView(ctx, &lsv1.GetHandoverViewRequest{Meta: meta(lsv1.Role_ROLE_FLIGHT_CONTROL, "fco")})
	if err != nil {
		t.Fatalf("handover before: %v", err)
	}
	ts.close()

	// "进程重启": 同一数据库重新拉起服务。
	clk = D0 + 200*sec
	ts2 := start(t, db, &clk)
	defer ts2.close()
	after, err := ts2.client.GetHandoverView(ctx, &lsv1.GetHandoverViewRequest{Meta: meta(lsv1.Role_ROLE_FLIGHT_CONTROL, "fco")})
	if err != nil {
		t.Fatalf("handover after: %v", err)
	}
	if len(after.Alerts) != 1 || after.Alerts[0].AckedBy != "fco-zhang" {
		t.Fatalf("after restart: %+v", after.Alerts)
	}
	// 确认人、形成采样、阈值版本在重启后保持一致。
	b, a := before.Alerts[0], after.Alerts[0]
	if a.AlertId != b.AlertId || a.FormedNs != b.FormedNs || a.ThresholdVersion != b.ThresholdVersion ||
		len(a.ContributingSamples) != len(b.ContributingSamples) || a.Owner != b.Owner {
		t.Fatalf("restart changed conclusions:\nbefore=%+v\nafter=%+v", b, a)
	}
}

func TestGRPCInsufficientEvidenceVisible(t *testing.T) {
	clk := D0 + 100*sec
	ts := start(t, ":memory:", &clk)
	defer ts.close()
	seedViaRPC(t, ts.client)
	ingestCO2(t, ts.client, "b1", 0, 10, 1.5)

	// 时钟前进到所有传感器失联。
	clk = D0 + 1000*sec
	env, err := ts.client.GetEnvironmentStatus(context.Background(), &lsv1.GetEnvironmentStatusRequest{
		Meta: meta(lsv1.Role_ROLE_CREW, "crew-1"),
	})
	if err != nil {
		t.Fatalf("env status: %v", err)
	}
	g := env.Groups[0]
	if g.Status != lsv1.EnvStatus_ENV_STATUS_INSUFFICIENT_EVIDENCE {
		t.Fatalf("status = %s, want INSUFFICIENT_EVIDENCE", g.Status)
	}
	if len(g.SilentSensors) == 0 {
		t.Fatal("silent sensors must be listed")
	}
}
