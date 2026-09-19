package engine

import (
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	lsv1 "lifesupport/gen/lifesupport/v1"
)

const sec = int64(time.Second)

// D0 设备时钟基准; 地面时刻 = 设备时刻 + 2s (模拟星地钟差)。
var D0 = int64(1_700_000_000) * sec

func ground(dev int64) int64 { return dev + 2*sec }

type rig struct {
	eng *Engine
}

func newRig(t *testing.T) *rig {
	t.Helper()
	eng, err := Open(":memory:", DefaultConfig(nil))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	r := &rig{eng: eng}
	r.apply(t, EvThresholds, DefaultThresholds(0), "seed:thr", ground(D0))
	r.apply(t, EvPhase, &lsv1.PhaseChangedEvent{
		Phase: lsv1.MissionPhase_MISSION_PHASE_ON_ORBIT, SetBy: "test",
	}, "seed:phase", ground(D0))
	return r
}

func (r *rig) apply(t *testing.T, typ string, msg proto.Message, uid string, arrival int64) *ApplyResult {
	t.Helper()
	res, err := r.eng.Apply(typ, msg, uid, arrival)
	if err != nil {
		t.Fatalf("apply %s: %v", typ, err)
	}
	return res
}

// smp 构造一条采样: 设备时刻 D0+offSec, 星上序号 = offSec (1Hz 遥测),
// 地面时刻晚 2s。
func (r *rig) smp(sensor string, offSec int64, v float64) *lsv1.Sample {
	return &lsv1.Sample{
		SensorId:     sensor,
		Seq:          offSec,
		DeviceTimeNs: D0 + offSec*sec,
		GroundRecvNs: ground(D0 + offSec*sec),
		Value:        v,
	}
}

// feedSeries 送入某传感器从 offSec 起 count 条 1Hz 采样, 到达时刻取末帧地面时刻。
func (r *rig) feedSeries(t *testing.T, sensor string, offSec, count int64, v float64) *ApplyResult {
	t.Helper()
	return r.feedSeriesArr(t, sensor, offSec, count, v, ground(D0+(offSec+count)*sec))
}

// feedSeriesArr 同上, 但显式给定批次到达时刻 (用于离线补传)。
func (r *rig) feedSeriesArr(t *testing.T, sensor string, offSec, count int64, v float64, arrival int64) *ApplyResult {
	t.Helper()
	var ss []*lsv1.Sample
	for i := int64(0); i < count; i++ {
		ss = append(ss, r.smp(sensor, offSec+i, v))
	}
	return r.apply(t, EvSampleBatch, &lsv1.SampleBatchEvent{Samples: ss}, "", arrival)
}

func (r *rig) alerts() []*lsv1.AlertView {
	return r.eng.ListAlertViews(true, ground(D0)+1_000_000*sec)
}

func TestSpikeDoesNotFormAlert(t *testing.T) {
	r := newRig(t)
	// 3 秒 CO2 尖峰 5.2 (窗口 60s): 不得形成告警。
	r.feedSeries(t, "co2_pp", 0, 3, 5.2)
	if got := r.alerts(); len(got) != 0 {
		t.Fatalf("spike formed %d alerts, want 0", len(got))
	}
	// 数据空洞会中断窗口: 30s 越限 + 空洞 + 30s 越限, 仍不形成告警。
	r.feedSeries(t, "co2_pp", 10, 30, 4.5)
	r.feedSeries(t, "co2_pp", 100, 30, 4.5) // 间隔 60s > max_gap 5s
	if got := r.alerts(); len(got) != 0 {
		t.Fatalf("gap-split breach formed %d alerts, want 0", len(got))
	}
}

func TestSustainedBreachFormsAlert(t *testing.T) {
	r := newRig(t)
	res := r.feedSeries(t, "co2_pp", 0, 61, 4.5) // 61s >= 60s 窗口
	if len(res.NewAlertIDs) != 1 {
		t.Fatalf("got %d new alerts, want 1", len(res.NewAlertIDs))
	}
	a := r.alerts()[0]
	if a.RuleId != "co2-red" || a.Severity != lsv1.Severity_SEVERITY_RED {
		t.Fatalf("unexpected alert: %s %s", a.RuleId, a.Severity)
	}
	if a.Status != lsv1.AlertStatus_ALERT_STATUS_OPEN {
		t.Fatalf("status = %s, want OPEN", a.Status)
	}
	// 形成时刻 = 区间起点 + 窗口 (设备时钟域)。
	if want := D0 + 60*sec; a.FormedNs != want {
		t.Fatalf("formed = %d, want %d", a.FormedNs, want)
	}
	if a.BreachStartNs != D0 || a.BreachEndNs != D0+60*sec {
		t.Fatalf("breach = [%d,%d]", a.BreachStartNs, a.BreachEndNs)
	}
	if a.ContributingSampleCount != 61 {
		t.Fatalf("contributing = %d, want 61", a.ContributingSampleCount)
	}
	if a.ThresholdVersion != "v1-onorbit" {
		t.Fatalf("version = %s", a.ThresholdVersion)
	}
	// 窗口满足后继续越限: 区间延伸但不产生新告警。
	r.feedSeries(t, "co2_pp", 61, 30, 4.6)
	if got := r.alerts(); len(got) != 1 {
		t.Fatalf("got %d alerts, want 1", len(got))
	}
	a = r.alerts()[0]
	if a.BreachEndNs != D0+90*sec || a.ContributingSampleCount != 91 {
		t.Fatalf("after extend: end=%d count=%d", a.BreachEndNs, a.ContributingSampleCount)
	}
}

func TestDuplicateFramesDoNotCount(t *testing.T) {
	r := newRig(t)
	r.feedSeries(t, "co2_pp", 0, 30, 4.5)
	// 重传 0-29 并多带一帧 30 (内容不同 => 不同幂等键): 旧 30 帧按星上序号去重。
	res := r.feedSeries(t, "co2_pp", 0, 31, 4.5)
	if res.Duplicates != 30 || res.Accepted != 1 {
		t.Fatalf("dup handling: accepted=%d duplicates=%d", res.Accepted, res.Duplicates)
	}
	if got := r.alerts(); len(got) != 0 {
		t.Fatalf("duplicates formed alert: %d", len(got))
	}
	// 继续送 30 条新帧后才应形成告警 (总计 61 条不同帧)。
	r.feedSeries(t, "co2_pp", 31, 30, 4.5)
	got := r.alerts()
	if len(got) != 1 || got[0].ContributingSampleCount != 61 {
		t.Fatalf("alerts=%d contributing=%v", len(got), got)
	}
	if got[0].FormedNs != D0+60*sec {
		t.Fatalf("formed=%d, want %d (重复帧不得提前窗口)", got[0].FormedNs, D0+60*sec)
	}
}

func TestBackfillMergesBySequence(t *testing.T) {
	r := newRig(t)
	r.feedSeries(t, "co2_pp", 0, 30, 4.5)
	r.feedSeries(t, "co2_pp", 50, 11, 4.5) // 中间缺 20s, 窗口断开
	if got := r.alerts(); len(got) != 0 {
		t.Fatalf("gap should prevent alert, got %d", len(got))
	}
	// 离线补传缺失的 20 帧 (星上序号 30-49): 按原始序列归并后窗口闭合。
	res := r.feedSeriesArr(t, "co2_pp", 30, 20, 4.5, ground(D0+70*sec))
	if len(res.NewAlertIDs) != 1 {
		t.Fatalf("backfill should complete window, new=%v", res.NewAlertIDs)
	}
	a := r.alerts()[0]
	if a.BreachStartNs != D0 || a.FormedNs != D0+60*sec {
		t.Fatalf("backfilled breach start=%d formed=%d", a.BreachStartNs, a.FormedNs)
	}
	// 告警的地面形成时刻 = 补传批次到达时刻, 晚于设备形成时刻 (可解释延迟)。
	if a.RaisedNs != ground(D0+70*sec) {
		t.Fatalf("raised = %d, want %d", a.RaisedNs, ground(D0+70*sec))
	}
}

func TestThresholdVersioning(t *testing.T) {
	r := newRig(t)
	// v1 下 CO2 注意阈值为 2.0: 1.9 不越限。
	r.feedSeries(t, "co2_pp", 0, 130, 1.9)
	if got := r.alerts(); len(got) != 0 {
		t.Fatalf("v1 should not alert on 1.9, got %d", len(got))
	}
	// 启用 v2 (注意阈值收紧到 1.8), 自设备时刻 D0+200s 生效。
	v2 := DefaultThresholds(D0 + 200*sec)
	v2.Version = "v2-stricter"
	for _, rule := range v2.Rules {
		if rule.RuleId == "co2-caution" {
			rule.Threshold = 1.8
		}
	}
	r.apply(t, EvThresholds, v2, "seed:thr2", ground(D0+200*sec))
	// v2 生效前的旧采样不得被新阈值重新判定。
	if got := r.alerts(); len(got) != 0 {
		t.Fatalf("v2 must not re-judge old samples, got %d alerts", len(got))
	}
	// v2 生效后的 1.9 持续 120s 形成注意告警。
	r.feedSeries(t, "co2_pp", 200, 121, 1.9)
	got := r.alerts()
	if len(got) != 1 {
		t.Fatalf("got %d alerts, want 1", len(got))
	}
	a := got[0]
	if a.ThresholdVersion != "v2-stricter" || a.RuleId != "co2-caution" {
		t.Fatalf("alert judged by %s/%s", a.ThresholdVersion, a.RuleId)
	}
	// 追溯性版本必须被拒绝。
	v3 := DefaultThresholds(D0 + 100*sec)
	v3.Version = "v3-retroactive"
	if _, err := r.eng.Apply(EvThresholds, v3, "seed:thr3", ground(D0+300*sec)); err == nil {
		t.Fatal("retroactive threshold version must be rejected")
	}
}

func TestMissionPhaseGating(t *testing.T) {
	r := newRig(t)
	// ON_ORBIT 下 3.5 只超过注意阈值(2.0)但不足 120s; 睡眠专用红线(3.0)不适用。
	r.feedSeries(t, "co2_pp", 0, 61, 3.5)
	if got := r.alerts(); len(got) != 0 {
		t.Fatalf("on-orbit: got %d alerts, want 0", len(got))
	}
	// 进入睡眠阶段 (自 D0+100s 起)。
	r.apply(t, EvPhase, &lsv1.PhaseChangedEvent{
		Phase: lsv1.MissionPhase_MISSION_PHASE_SLEEP, EffectiveFromDeviceNs: D0 + 100*sec, SetBy: "test",
	}, "phase:sleep", ground(D0+100*sec))
	r.feedSeries(t, "co2_pp", 100, 61, 3.5)
	got := r.alerts()
	if len(got) != 1 {
		t.Fatalf("sleep: got %d alerts, want 1", len(got))
	}
	if got[0].RuleId != "co2-red-sleep" || got[0].Phase != lsv1.MissionPhase_MISSION_PHASE_SLEEP {
		t.Fatalf("got %s in %s", got[0].RuleId, got[0].Phase)
	}
}

func TestSensorLossIsInsufficientEvidence(t *testing.T) {
	r := newRig(t)
	// 只送两个传感器, 风扇从未出现。
	r.feedSeries(t, "co2_pp", 0, 10, 1.5)
	r.feedSeries(t, "cabin_pressure", 0, 10, 101.3)
	now := ground(D0 + 20*sec) // co2/舱压 10s 前仍在线; 风扇从未在线
	env := r.eng.EnvironmentStatus(now)
	g := env.Groups[0]
	if g.Status != lsv1.EnvStatus_ENV_STATUS_INSUFFICIENT_EVIDENCE {
		t.Fatalf("status = %s, want INSUFFICIENT_EVIDENCE (不能推导安全)", g.Status)
	}
	if len(g.SilentSensors) != 1 || g.SilentSensors[0] != "fan_speed" {
		t.Fatalf("silent = %v", g.SilentSensors)
	}
	// 风扇恢复后, 无告警 => 正常。
	r.feedSeries(t, "fan_speed", 0, 10, 1200)
	now = ground(D0 + 12*sec)
	env = r.eng.EnvironmentStatus(now)
	if env.Groups[0].Status != lsv1.EnvStatus_ENV_STATUS_NOMINAL {
		t.Fatalf("status = %s, want NOMINAL", env.Groups[0].Status)
	}
	// 任一传感器再次失联 => 再次证据不足。
	now = ground(D0 + 60*sec)
	env = r.eng.EnvironmentStatus(now)
	if env.Groups[0].Status != lsv1.EnvStatus_ENV_STATUS_INSUFFICIENT_EVIDENCE {
		t.Fatalf("status = %s after loss, want INSUFFICIENT_EVIDENCE", env.Groups[0].Status)
	}
}

func TestAlertLifecycleAndImmutability(t *testing.T) {
	r := newRig(t)
	r.feedSeries(t, "co2_pp", 0, 61, 4.5)
	id := r.alerts()[0].AlertId
	at := ground(D0 + 100*sec)

	// 红色告警未确认前不得解除。
	if _, err := r.eng.Apply(EvResolve, &lsv1.ResolveEvent{
		AlertId: id, Operator: "fco", Summary: "x",
	}, "", at); err == nil {
		t.Fatal("red alert resolve without ack must fail")
	}
	// 确认 (显式幂等键)。
	r.apply(t, EvAck, &lsv1.AckEvent{AlertId: id, Operator: "fco-zhang", Role: lsv1.Role_ROLE_FLIGHT_CONTROL}, "ack:1", at)
	a := r.eng.AlertViewByID(id, at)
	if a.Status != lsv1.AlertStatus_ALERT_STATUS_ACKED || a.Owner != "fco-zhang" || a.AckedBy != "fco-zhang" {
		t.Fatalf("after ack: %s owner=%s", a.Status, a.Owner)
	}
	// 同一幂等键重试 => 安全忽略, 状态不变。
	res2, err := r.eng.Apply(EvAck, &lsv1.AckEvent{AlertId: id, Operator: "fco-zhang"}, "ack:1", at)
	if err != nil || !res2.DuplicateRequest {
		t.Fatalf("duplicate ack:1: res=%v err=%v", res2, err)
	}
	// 交班接管。
	r.apply(t, EvAssign, &lsv1.AssignEvent{AlertId: id, Operator: "fco-li", Role: lsv1.Role_ROLE_FLIGHT_CONTROL}, "", at+10*sec)
	a = r.eng.AlertViewByID(id, at+10*sec)
	if a.Owner != "fco-li" {
		t.Fatalf("owner = %s, want fco-li", a.Owner)
	}
	// 解除必须带证据摘要。
	if _, err := r.eng.Apply(EvResolve, &lsv1.ResolveEvent{AlertId: id, Operator: "fco-li"}, "", at+20*sec); err == nil {
		t.Fatal("resolve without evidence summary must fail")
	}
	r.apply(t, EvResolve, &lsv1.ResolveEvent{
		AlertId: id, Operator: "fco-li", Summary: "已更换清除罐, 指标恢复正常",
	}, "", at+30*sec)
	a = r.eng.AlertViewByID(id, at+30*sec)
	if a.Status != lsv1.AlertStatus_ALERT_STATUS_RESOLVED || a.ResolvedBy != "fco-li" {
		t.Fatalf("after resolve: %s by %s", a.Status, a.ResolvedBy)
	}
	// 已解除告警只允许追加证据。
	r.apply(t, EvEvidence, &lsv1.EvidenceEvent{
		AlertId: id, Operator: "med-wang", Summary: "医学复核: 乘组无高碳酸血症症状",
	}, "", at+40*sec)
	a = r.eng.AlertViewByID(id, at+40*sec)
	if len(a.Evidence) != 2 {
		t.Fatalf("evidence = %d, want 2 (解除证据+医学复核)", len(a.Evidence))
	}
	if _, err := r.eng.Apply(EvAck, &lsv1.AckEvent{AlertId: id, Operator: "x"}, "", at+50*sec); err == nil {
		t.Fatal("ack on resolved alert must fail")
	}
	if _, err := r.eng.Apply(EvAssign, &lsv1.AssignEvent{AlertId: id, Operator: "x"}, "", at+50*sec); err == nil {
		t.Fatal("assign on resolved alert must fail")
	}
	// 未知告警。
	if _, err := r.eng.Apply(EvAck, &lsv1.AckEvent{AlertId: "ALR-9999", Operator: "x"}, "", at); err == nil {
		t.Fatal("ack on unknown alert must fail")
	}
}

func TestEscalationSchedule(t *testing.T) {
	r := newRig(t)
	r.feedSeries(t, "co2_pp", 0, 61, 4.5) // 红色, raised = ground(D0+61s)
	raised := ground(D0 + 61*sec)
	id := r.alerts()[0].AlertId

	// 未确认: 每 5 分钟升一级, 封顶 3 级。
	a := r.eng.AlertViewByID(id, raised)
	if a.EscalationLevel != 1 || a.NextEscalationNs != raised+300*sec {
		t.Fatalf("t0: level=%d next=%d", a.EscalationLevel, a.NextEscalationNs)
	}
	a = r.eng.AlertViewByID(id, raised+299*sec)
	if a.EscalationLevel != 1 {
		t.Fatalf("t+299s: level=%d, want 1", a.EscalationLevel)
	}
	a = r.eng.AlertViewByID(id, raised+300*sec)
	if a.EscalationLevel != 2 || a.EscalationHistory[1].NotifiedRole != "值班主任" {
		t.Fatalf("t+300s: level=%d history=%v", a.EscalationLevel, a.EscalationHistory)
	}
	a = r.eng.AlertViewByID(id, raised+600*sec)
	if a.EscalationLevel != 3 || a.NextEscalationNs != 0 {
		t.Fatalf("t+600s: level=%d next=%d (封顶)", a.EscalationLevel, a.NextEscalationNs)
	}
	if a.NsToNextEscalation != 0 {
		t.Fatalf("max level should have no next escalation")
	}
	// 确认后升级冻结, 历史保留。
	r.apply(t, EvAck, &lsv1.AckEvent{AlertId: id, Operator: "fco", Role: lsv1.Role_ROLE_FLIGHT_CONTROL}, "", raised+350*sec)
	a = r.eng.AlertViewByID(id, raised+10_000*sec)
	if a.EscalationLevel != 2 || len(a.EscalationHistory) != 2 || a.NextEscalationNs != 0 {
		t.Fatalf("after ack: level=%d history=%d next=%d",
			a.EscalationLevel, len(a.EscalationHistory), a.NextEscalationNs)
	}
}

func TestIdempotentIngest(t *testing.T) {
	r := newRig(t)
	batch := &lsv1.SampleBatchEvent{Samples: []*lsv1.Sample{r.smp("co2_pp", 0, 1.5)}}
	res1, err := r.eng.Apply(EvSampleBatch, batch, "ingest:abc", ground(D0))
	if err != nil || res1.Accepted != 1 {
		t.Fatalf("first: %+v err=%v", res1, err)
	}
	res2, err := r.eng.Apply(EvSampleBatch, batch, "ingest:abc", ground(D0))
	if err != nil {
		t.Fatalf("duplicate key: %v", err)
	}
	if !res2.DuplicateRequest || res2.Accepted != 0 {
		t.Fatalf("duplicate: %+v", res2)
	}
	// 同样的帧换个键再送: 事件被记录, 但采样按星上序号去重。
	res3, err := r.eng.Apply(EvSampleBatch, batch, "ingest:abd", ground(D0))
	if err != nil || res3.Duplicates != 1 {
		t.Fatalf("re-delivery: %+v err=%v", res3, err)
	}
	if n := len(r.eng.sensors["co2_pp"].samples); n != 1 {
		t.Fatalf("samples stored = %d, want 1", n)
	}
}

// TestReplayAcrossReopen 在同一数据库上中断-重开, 状态必须一致。
func TestReplayAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "events.db")

	feed := func(eng *Engine) {
		seq := int64(0)
		for i := int64(0); i < 61; i++ {
			seq++
			_, err := eng.Apply(EvSampleBatch, &lsv1.SampleBatchEvent{Samples: []*lsv1.Sample{{
				SensorId: "co2_pp", Seq: seq, DeviceTimeNs: D0 + i*sec,
				GroundRecvNs: ground(D0 + i*sec), Value: 4.5,
			}}}, "", ground(D0+i*sec))
			if err != nil {
				t.Fatalf("feed: %v", err)
			}
		}
	}

	eng1, err := Open(db, DefaultConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng1.Apply(EvThresholds, DefaultThresholds(0), "seed:thr", ground(D0)); err != nil {
		t.Fatal(err)
	}
	feed(eng1)
	if _, err := eng1.Apply(EvAck, &lsv1.AckEvent{AlertId: "ALR-0001", Operator: "fco-a"}, "", ground(D0+70*sec)); err != nil {
		t.Fatal(err)
	}
	dump1 := eng1.DumpState(ground(D0 + 1000*sec))
	eng1.Close()

	eng2, err := Open(db, DefaultConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer eng2.Close()
	dump2 := eng2.DumpState(ground(D0 + 1000*sec))
	if dump1 != dump2 {
		t.Fatalf("reopen replay mismatch:\n--- before ---\n%s\n--- after ---\n%s", dump1, dump2)
	}
}
