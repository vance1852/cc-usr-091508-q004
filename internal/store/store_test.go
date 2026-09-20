package store

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	pb "lifesupport/gen/lifesupport/v1"
	"lifesupport/internal/core"
)

const sec = int64(time.Second)
const min = 60 * sec

// T0 为测试基准时刻（设备时钟与地面时钟的共同原点）。
const T0 = int64(1_700_000_000_000_000_000)

func smk(sensor string, seq int64, devNs, groundNs int64, val float64) core.Sample {
	return core.Sample{
		SensorID: sensor, Seq: seq, FrameID: fmt.Sprintf("%s#%d", sensor, seq),
		DeviceNs: devNs, GroundNs: groundNs, Value: val,
	}
}

// co2Series 生成 CO2 双通道采样：主通道延迟 5s、关联通道延迟 7s 到达地面。
func co2Series(seqStart int, devStart int64, n int, primaryVal, secondaryVal float64) []core.Sample {
	var out []core.Sample
	for i := 0; i < n; i++ {
		dev := devStart + int64(int64(seqStart-1+i)*10)*sec
		out = append(out,
			smk("co2_primary", int64(seqStart+i), dev, dev+5*sec, primaryVal),
			smk("co2_secondary", int64(seqStart+i), dev, dev+7*sec, secondaryVal),
		)
	}
	return out
}

func pressureSeries(seqStart int, devStart int64, n int, val float64) []core.Sample {
	var out []core.Sample
	for i := 0; i < n; i++ {
		dev := devStart + int64(int64(seqStart-1+i)*10)*sec
		out = append(out,
			smk("cabin_pressure", int64(seqStart+i), dev, dev+8*sec, val),
			smk("cabin_pressure_backup", int64(seqStart+i), dev, dev+9*sec, val+0.1),
		)
	}
	return out
}

func newStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func setupConfig(t *testing.T, st *Store) {
	t.Helper()
	if err := st.RegisterThresholdVersion(core.DefaultThresholdVersion(), T0); err != nil {
		t.Fatalf("注册阈值版本失败: %v", err)
	}
	if err := st.SetMissionPhase(pb.MissionPhase_MISSION_PHASE_ON_ORBIT, 0, T0); err != nil {
		t.Fatalf("设置任务阶段失败: %v", err)
	}
}

func mustAlerts(t *testing.T, st *Store) []Alert {
	t.Helper()
	alerts, err := st.ListAlerts()
	if err != nil {
		t.Fatalf("查询告警失败: %v", err)
	}
	return alerts
}

func TestSpikeDoesNotOpenAlert(t *testing.T) {
	st := newStore(t)
	setupConfig(t, st)
	// 背景正常值，中间一个 6.5 的尖峰。
	var samples []core.Sample
	samples = append(samples, co2Series(1, T0, 15, 2.0, 2.0)...)
	samples = append(samples,
		smk("co2_primary", 16, T0+150*sec, T0+155*sec, 6.5),
		smk("co2_secondary", 16, T0+150*sec, T0+157*sec, 2.1))
	samples = append(samples, co2Series(17, T0, 15, 2.0, 2.0)...)
	if _, err := st.IngestSamples(samples, "spike-1"); err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	if got := len(mustAlerts(t, st)); got != 0 {
		t.Fatalf("短暂尖峰不得形成告警，得到 %d 条", got)
	}
	// 判定记录中应能看到 TRANSIENT 结论（审计留痕）。
	evals, err := st.ListEvaluations("co2_high", 200)
	if err != nil {
		t.Fatalf("查询判定失败: %v", err)
	}
	seenTransient := false
	for _, e := range evals {
		if e.Verdict == pb.Verdict_VERDICT_TRANSIENT {
			seenTransient = true
		}
		if e.Verdict == pb.Verdict_VERDICT_SUSTAINED_BREACH {
			t.Fatalf("尖峰不得判定为持续越限")
		}
	}
	if !seenTransient {
		t.Fatalf("应记录到 TRANSIENT 判定")
	}
}

func TestSustainedBreachOpensAlertWithEvidence(t *testing.T) {
	st := newStore(t)
	setupConfig(t, st)
	if _, err := st.IngestSamples(co2Series(1, T0, 40, 5.5, 5.4), "b1"); err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	alerts := mustAlerts(t, st)
	if len(alerts) != 1 {
		t.Fatalf("应形成 1 条告警，得到 %d", len(alerts))
	}
	a := alerts[0]
	if a.Rule != "co2_high" || a.Severity != pb.Severity_SEVERITY_CRITICAL || a.State != pb.AlertState_ALERT_STATE_OPEN {
		t.Fatalf("告警字段不符: %+v", a)
	}
	if a.OpenedDeviceNs != T0 {
		t.Fatalf("越限段起点应为 T0，得到 %d", a.OpenedDeviceNs)
	}
	// 红警窗口 300s：在第 31 个采样（设备时刻 T0+300s）开启，地面时标为其接收时刻。
	if a.OpenedGroundNs != T0+300*sec+5*sec {
		t.Fatalf("开启地面时刻应为 T0+305s，得到 %d", a.OpenedGroundNs)
	}
	if a.ThresholdVersion != "v1-on-orbit" || a.Phase != pb.MissionPhase_MISSION_PHASE_ON_ORBIT {
		t.Fatalf("告警应记录阈值版本与任务阶段: %+v", a)
	}
	ev, err := st.AlertEvidence(a.ID)
	if err != nil {
		t.Fatalf("查询证据失败: %v", err)
	}
	if len(ev) != 40 {
		t.Fatalf("证据应覆盖全部 40 个越限采样，得到 %d", len(ev))
	}
	// 未确认红警 2 分钟后应升级到 L1。
	if a.NextEscGroundNs != a.OpenedGroundNs+2*min {
		t.Fatalf("下一次升级时刻应为开启后 2 分钟，得到 %d", a.NextEscGroundNs)
	}
}

func TestDuplicateFramesAndIdempotentSubmission(t *testing.T) {
	st := newStore(t)
	setupConfig(t, st)
	batch := co2Series(1, T0, 5, 2.0, 2.0)
	res1, err := st.IngestSamples(batch, "sub-1")
	if err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	if res1.Accepted != 10 || res1.Duplicates != 0 {
		t.Fatalf("首批应全部接受: %+v", res1)
	}
	// 同一批原样重投（进程中断后重发）：逐帧去重。
	res2, err := st.IngestSamples(batch, "sub-1-retry")
	if err != nil {
		t.Fatalf("重投失败: %v", err)
	}
	if res2.Accepted != 0 || res2.Duplicates != 10 {
		t.Fatalf("重投应全部去重: %+v", res2)
	}
	// 相同 submission_id：直接返回首次结果。
	res3, err := st.IngestSamples(batch, "sub-1")
	if err != nil {
		t.Fatalf("幂等重投失败: %v", err)
	}
	if !res3.Replayed || res3.Accepted != 10 {
		t.Fatalf("相同 submission_id 应返回首次结果: %+v", res3)
	}
}

func TestConflictingSequenceKeepsFirst(t *testing.T) {
	st := newStore(t)
	setupConfig(t, st)
	s1 := smk("co2_primary", 1, T0, T0+5*sec, 2.0)
	if _, err := st.IngestSamples([]core.Sample{s1}, "c1"); err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	// 同序列不同取值（不同帧）：保留先到的，记冲突。
	s2 := smk("co2_primary", 1, T0, T0+6*sec, 9.9)
	s2.FrameID = "co2_primary#1-alt"
	res, err := st.IngestSamples([]core.Sample{s2}, "c2")
	if err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	if res.Conflicts != 1 || res.Accepted != 0 {
		t.Fatalf("应记录 1 起冲突: %+v", res)
	}
	health, err := st.SensorHealth()
	if err != nil {
		t.Fatalf("查询传感器健康失败: %v", err)
	}
	if len(health) != 1 || health[0].ConflictCount != 1 {
		t.Fatalf("冲突计数应为 1: %+v", health)
	}
}

func TestBackfillMergesByOriginalSequence(t *testing.T) {
	// 全部采样（40 对 CO2 双通道）。
	all := co2Series(1, T0, 40, 5.5, 5.4)

	// A：顺序到达。
	stA := newStore(t)
	setupConfig(t, stA)
	if _, err := stA.IngestSamples(all, "a"); err != nil {
		t.Fatalf("A 注入失败: %v", err)
	}

	// B：离线补传，乱序分批到达（后半先到，含重复帧）。
	stB := newStore(t)
	setupConfig(t, stB)
	var first, second, third []core.Sample
	for _, s := range all {
		dev := s.DeviceNs
		switch {
		case dev >= T0+200*sec:
			second = append(second, s)
		default:
			first = append(first, s)
		}
	}
	third = append(third, first...) // 重复帧随补传重发
	if _, err := stB.IngestSamples(second, "b-late"); err != nil {
		t.Fatalf("B 后半注入失败: %v", err)
	}
	if _, err := stB.IngestSamples(first, "b-backfill"); err != nil {
		t.Fatalf("B 补传注入失败: %v", err)
	}
	if _, err := stB.IngestSamples(third, "b-redelivery"); err != nil {
		t.Fatalf("B 重发注入失败: %v", err)
	}

	alertsA := mustAlerts(t, stA)
	alertsB := mustAlerts(t, stB)
	if len(alertsA) != 1 || len(alertsB) != 1 {
		t.Fatalf("两侧应各形成 1 条告警: A=%d B=%d", len(alertsA), len(alertsB))
	}
	a, b := alertsA[0], alertsB[0]
	// 结论字段必须一致：同一事件、同一越限段、同一严重度。
	if a.ID != b.ID || a.Rule != b.Rule || a.Severity != b.Severity || a.OpenedDeviceNs != b.OpenedDeviceNs {
		t.Fatalf("补传归并后告警结论不一致:\nA=%+v\nB=%+v", a, b)
	}
	evA, _ := stA.AlertEvidence(a.ID)
	evB, _ := stB.AlertEvidence(b.ID)
	if len(evA) != len(evB) {
		t.Fatalf("证据采样数不一致: A=%d B=%d", len(evA), len(evB))
	}
	for i := range evA {
		if evA[i].SensorID != evB[i].SensorID || evA[i].Seq != evB[i].Seq || evA[i].Value != evB[i].Value {
			t.Fatalf("证据采样 %d 不一致: A=%+v B=%+v", i, evA[i], evB[i])
		}
	}
}

func TestSensorLossMarksInsufficientNeverResolves(t *testing.T) {
	st := newStore(t)
	setupConfig(t, st)
	// 先形成红警。
	if _, err := st.IngestSamples(co2Series(1, T0, 40, 5.5, 5.4), "s1"); err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	alerts := mustAlerts(t, st)
	if len(alerts) != 1 {
		t.Fatalf("应形成告警")
	}
	id := alerts[0].ID

	// 关联传感器失联：主传感器继续越限，但 co2_secondary 不再有数据。
	var primaryOnly []core.Sample
	for i := 41; i <= 60; i++ {
		dev := T0 + int64(i-1)*10*sec
		primaryOnly = append(primaryOnly, smk("co2_primary", int64(i), dev, dev+5*sec, 5.6))
	}
	if _, err := st.IngestSamples(primaryOnly, "s2"); err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	a, err := st.GetAlert(id)
	if err != nil {
		t.Fatalf("查询告警失败: %v", err)
	}
	if !a.Insufficient {
		t.Fatalf("关联传感器失联应标记证据不足")
	}
	if len(a.Missing) != 1 || a.Missing[0] != "co2_secondary" {
		t.Fatalf("缺失关联传感器应为 co2_secondary: %v", a.Missing)
	}
	if a.State == pb.AlertState_ALERT_STATE_RESOLVED {
		t.Fatalf("证据不足绝不允许解除告警")
	}
	// 失联期间不得出现“安全”结论：判定记录里不允许有 NOMINAL。
	evals, _ := st.ListEvaluations("co2_high", 500)
	for _, e := range evals {
		if e.EndDeviceNs >= T0+400*sec && e.Verdict == pb.Verdict_VERDICT_NOMINAL {
			t.Fatalf("关联传感器失联期间不得判定 NOMINAL（设备时刻 %d）", e.EndDeviceNs)
		}
	}

	// 关联传感器恢复后，证据不足标记清除，告警延续。
	if _, err := st.IngestSamples(co2Series(61, T0, 5, 5.6, 5.5), "s3"); err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	a, _ = st.GetAlert(id)
	if a.Insufficient {
		t.Fatalf("关联传感器恢复后应清除证据不足标记")
	}
	if a.State != pb.AlertState_ALERT_STATE_OPEN {
		t.Fatalf("告警应仍处于 OPEN: %v", a.State)
	}
}

func TestAutoRecoveryAppendsSystemResolution(t *testing.T) {
	st := newStore(t)
	setupConfig(t, st)
	if _, err := st.IngestSamples(co2Series(1, T0, 40, 5.5, 5.4), "r1"); err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	id := mustAlerts(t, st)[0].ID
	// 随后 40 个采样恢复正常（恢复窗口 5 分钟）。
	if _, err := st.IngestSamples(co2Series(41, T0, 40, 2.0, 2.0), "r2"); err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	a, _ := st.GetAlert(id)
	if a.State != pb.AlertState_ALERT_STATE_RESOLVED {
		t.Fatalf("持续恢复正常满窗口后应自动解除，状态 %v", a.State)
	}
	res, _ := st.ListResolutions(id)
	if len(res) != 1 || !res[0].Auto || res[0].By != "system" {
		t.Fatalf("应追加一条系统自动解除证据: %+v", res)
	}
}

func TestAcknowledgeTransferAndHistory(t *testing.T) {
	st := newStore(t)
	setupConfig(t, st)
	if _, err := st.IngestSamples(co2Series(1, T0, 40, 5.5, 5.4), "k1"); err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	id := mustAlerts(t, st)[0].ID

	a, err := st.Acknowledge(id, "li-wei", pb.Role_ROLE_FLIGHT_CONTROL, "值班接管", T0+400*sec)
	if err != nil {
		t.Fatalf("确认失败: %v", err)
	}
	if a.State != pb.AlertState_ALERT_STATE_ACKNOWLEDGED || a.AckBy != "li-wei" {
		t.Fatalf("确认状态错误: %+v", a)
	}
	// 移交他人：历史保留两任接管人。
	if _, err := st.Acknowledge(id, "wang-fang", pb.Role_ROLE_FLIGHT_CONTROL, "交班接管", T0+500*sec); err != nil {
		t.Fatalf("移交失败: %v", err)
	}
	// 同人重复确认幂等。
	if _, err := st.Acknowledge(id, "wang-fang", pb.Role_ROLE_FLIGHT_CONTROL, "重复", T0+510*sec); err != nil {
		t.Fatalf("重复确认失败: %v", err)
	}
	a, _ = st.GetAlert(id)
	if a.AckBy != "wang-fang" {
		t.Fatalf("当前接管人应为 wang-fang: %s", a.AckBy)
	}
	acks, _ := st.ListAcknowledgements(id)
	if len(acks) != 2 || acks[0].By != "li-wei" || acks[1].By != "wang-fang" {
		t.Fatalf("确认历史应为 li-wei -> wang-fang: %+v", acks)
	}
}

func TestResolutionIsAppendOnly(t *testing.T) {
	st := newStore(t)
	setupConfig(t, st)
	if _, err := st.IngestSamples(co2Series(1, T0, 40, 5.5, 5.4), "x1"); err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	id := mustAlerts(t, st)[0].ID
	if _, err := st.Acknowledge(id, "li-wei", pb.Role_ROLE_FLIGHT_CONTROL, "", T0+400*sec); err != nil {
		t.Fatalf("确认失败: %v", err)
	}
	if _, err := st.AppendResolution(id, "li-wei", pb.Role_ROLE_FLIGHT_CONTROL, "启用备用清除装置后 CO2 回落至 2.1", T0+600*sec); err != nil {
		t.Fatalf("追加解除证据失败: %v", err)
	}
	// 已解除后仍可继续追加证据。
	a, err := st.AppendResolution(id, "dr-chen", pb.Role_ROLE_MEDICAL, "医学复核乘组无高碳酸血症体征", T0+700*sec)
	if err != nil {
		t.Fatalf("二次追加失败: %v", err)
	}
	if a.State != pb.AlertState_ALERT_STATE_RESOLVED {
		t.Fatalf("状态应保持 RESOLVED")
	}
	res, _ := st.ListResolutions(id)
	if len(res) != 2 {
		t.Fatalf("解除证据链应有 2 条: %+v", res)
	}
	if a.ResolvedGroundNs != T0+600*sec {
		t.Fatalf("解除时刻以首次证据为准: %d", a.ResolvedGroundNs)
	}
	// 已解除的告警不允许再确认。
	if _, err := st.Acknowledge(id, "someone", pb.Role_ROLE_FLIGHT_CONTROL, "", T0+800*sec); err == nil {
		t.Fatalf("已解除告警不应允许确认")
	}
}

func TestEscalationFiresOnSchedule(t *testing.T) {
	st := newStore(t)
	setupConfig(t, st)
	if _, err := st.IngestSamples(co2Series(1, T0, 40, 5.5, 5.4), "e1"); err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	id := mustAlerts(t, st)[0].ID
	g0 := T0 + 305*sec // 告警开启地面时刻

	fire := func(ground int64) {
		t.Helper()
		if err := st.Heartbeat(ground); err != nil {
			t.Fatalf("心跳失败: %v", err)
		}
	}
	fire(g0 + 1*min)
	escs, _ := st.ListEscalations(id)
	if len(escs) != 0 {
		t.Fatalf("未到时不应升级: %+v", escs)
	}
	fire(g0 + 2*min) // L1 到期
	fire(g0 + 6*min) // L2 到期
	escs, _ = st.ListEscalations(id)
	if len(escs) != 2 || escs[0].Level != 1 || escs[1].Level != 2 {
		t.Fatalf("应依次触发 L1/L2: %+v", escs)
	}
	if escs[0].DueGroundNs != g0+2*min || escs[1].DueGroundNs != g0+5*min {
		t.Fatalf("升级应到时刻错误: %+v", escs)
	}
	// 确认后未确认链停止，切换为未解除督办（30 分钟）。
	if _, err := st.Acknowledge(id, "li-wei", pb.Role_ROLE_FLIGHT_CONTROL, "", g0+7*min); err != nil {
		t.Fatalf("确认失败: %v", err)
	}
	fire(g0 + 12*min)
	escs, _ = st.ListEscalations(id)
	if len(escs) != 2 {
		t.Fatalf("确认后不应再触发未确认链: %+v", escs)
	}
	fire(g0 + 7*min + 31*min) // 确认后 31 分钟
	escs, _ = st.ListEscalations(id)
	if len(escs) != 3 || escs[2].Level != 4 {
		t.Fatalf("应触发未解除督办 L4: %+v", escs)
	}
}

func TestThresholdVersionImmutableAndPinned(t *testing.T) {
	st := newStore(t)
	setupConfig(t, st)
	if _, err := st.IngestSamples(co2Series(1, T0, 40, 5.5, 5.4), "v1"); err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	id := mustAlerts(t, st)[0].ID

	// 相同配置重复注册：幂等无操作。
	if err := st.RegisterThresholdVersion(core.DefaultThresholdVersion(), T0+1000*sec); err != nil {
		t.Fatalf("幂等重注册失败: %v", err)
	}
	// 同版本号不同配置：拒绝（版本不可变）。
	mutated := core.DefaultThresholdVersion()
	mutated.Rules[0].CriticalLimit = 9.9
	if err := st.RegisterThresholdVersion(mutated, T0+1000*sec); err == nil {
		t.Fatalf("同版本不同配置必须拒绝")
	}

	// 注册更严格的新版本（在未来生效）。
	v2 := core.DefaultThresholdVersion()
	v2.Version = "v2-stricter"
	v2.EffectiveDeviceNs = T0 + 2000*sec
	for i := range v2.Rules {
		if v2.Rules[i].Name == "co2_high" {
			v2.Rules[i].CriticalLimit = 4.0
		}
	}
	if err := st.RegisterThresholdVersion(v2, T0+2000*sec); err != nil {
		t.Fatalf("注册 v2 失败: %v", err)
	}

	// 当时的结论不受影响：告警仍引用 v1，旧判定记录保持 v1。
	a, _ := st.GetAlert(id)
	if a.ThresholdVersion != "v1-on-orbit" {
		t.Fatalf("旧告警的阈值版本不得被改写: %s", a.ThresholdVersion)
	}
	evals, _ := st.ListEvaluations("co2_high", 1000)
	for _, e := range evals {
		if e.EndDeviceNs < T0+2000*sec && e.ThresholdVersion != "v1-on-orbit" {
			t.Fatalf("旧窗口判定必须保持 v1: %+v", e)
		}
	}
	// 新版本生效后的越限按 v2 判定（红限 4.0，4.5 即可构成红警）。
	if _, err := st.IngestSamples(co2Series(200, T0+2000*sec, 40, 4.5, 4.4), "v2"); err != nil {
		t.Fatalf("v2 注入失败: %v", err)
	}
	var found bool
	evals, _ = st.ListEvaluations("co2_high", 1000)
	for _, e := range evals {
		if e.ThresholdVersion == "v2-stricter" && e.Verdict == pb.Verdict_VERDICT_SUSTAINED_BREACH {
			found = true
		}
	}
	if !found {
		t.Fatalf("v2 生效后应按 v2 判定出持续越限")
	}
}

func TestMissionPhaseOverrideApplies(t *testing.T) {
	st := newStore(t)
	setupConfig(t, st)
	// EVA 自 T0+1000s 起。
	if err := st.SetMissionPhase(pb.MissionPhase_MISSION_PHASE_EVA, T0+1000*sec, T0+1000*sec); err != nil {
		t.Fatalf("设置阶段失败: %v", err)
	}
	// CO2 3.2 在在轨黄限 3.0 之上、EVA 黄限 3.5 之下：EVA 期间持续 3.2 不得告警。
	if _, err := st.IngestSamples(co2Series(1, T0+1000*sec, 80, 3.2, 3.1), "p1"); err != nil {
		t.Fatalf("注入失败: %v", err)
	}
	if got := len(mustAlerts(t, st)); got != 0 {
		t.Fatalf("EVA 阶段 3.2 未越覆盖后的黄限，不应告警，得到 %d 条", got)
	}
}

// TestRebuildIsDeterministic 是重放稳定性的核心验证：
// 事件流包含时钟漂移、重复帧、冲突帧、乱序补传、进程中断与配置变更，
// 重建后未决告警、升级历史与确认人必须稳定一致。
func TestRebuildIsDeterministic(t *testing.T) {
	ops := func(st *Store) {
		setupConfig(t, st)

		// 批次 1：CO2 越限，含时钟回退（漂移）、重复帧、冲突帧。
		b1 := co2Series(1, T0, 20, 5.5, 5.4)
		b1[18].DeviceNs = b1[16].DeviceNs - 30*sec // co2_primary seq 10 设备时钟回退
		dup := smk("co2_primary", 5, T0+40*sec, T0+45*sec, 5.5)
		conflict := smk("co2_primary", 8, T0+70*sec, T0+75*sec, 9.9)
		conflict.FrameID = "co2_primary#8-alt"
		b1 = append(b1, dup, conflict)
		if _, err := st.IngestSamples(b1, "rb-1"); err != nil {
			t.Fatalf("rb-1 注入失败: %v", err)
		}

		// 批次 2：乱序补传（后半先到）。
		b2 := co2Series(21, T0, 20, 5.5, 5.4)
		if _, err := st.IngestSamples(b2, "rb-2"); err != nil {
			t.Fatalf("rb-2 注入失败: %v", err)
		}

		// 升级推进 + 确认。
		if err := st.Heartbeat(T0 + 500*sec); err != nil {
			t.Fatalf("心跳失败: %v", err)
		}
		id := mustAlerts(t, st)[0].ID
		if _, err := st.Acknowledge(id, "li-wei", pb.Role_ROLE_FLIGHT_CONTROL, "值班接管", T0+510*sec); err != nil {
			t.Fatalf("确认失败: %v", err)
		}

		// 批次 3：恢复正常（自动解除）。
		if _, err := st.IngestSamples(co2Series(41, T0, 40, 2.0, 2.0), "rb-3"); err != nil {
			t.Fatalf("rb-3 注入失败: %v", err)
		}

		// 舱压告警，随后人工追加解除证据。
		if _, err := st.IngestSamples(pressureSeries(1, T0+600*sec, 30, 88.0), "rb-4"); err != nil {
			t.Fatalf("rb-4 注入失败: %v", err)
		}
		var pressureID string
		for _, a := range mustAlerts(t, st) {
			if a.Rule == "cabin_pressure_low" {
				pressureID = a.ID
			}
		}
		if pressureID == "" {
			t.Fatalf("应形成舱压告警")
		}
		if _, err := st.AppendResolution(pressureID, "dr-chen", pb.Role_ROLE_MEDICAL,
			"舱压回升至 101.2kPa 并复核传感器一致", T0+1100*sec); err != nil {
			t.Fatalf("追加解除证据失败: %v", err)
		}

		// 阶段切换 + 新阈值版本 + 新一轮 CO2 越限（未决，保持到快照）。
		if err := st.SetMissionPhase(pb.MissionPhase_MISSION_PHASE_EVA, T0+2000*sec, T0+2000*sec); err != nil {
			t.Fatalf("阶段切换失败: %v", err)
		}
		v2 := core.DefaultThresholdVersion()
		v2.Version = "v2-stricter"
		v2.EffectiveDeviceNs = T0 + 2100*sec
		if err := st.RegisterThresholdVersion(v2, T0+2100*sec); err != nil {
			t.Fatalf("注册 v2 失败: %v", err)
		}
		if _, err := st.IngestSamples(co2Series(100, T0+2200*sec, 40, 5.5, 5.4), "rb-5"); err != nil {
			t.Fatalf("rb-5 注入失败: %v", err)
		}
		if err := st.Heartbeat(T0 + 2700*sec); err != nil {
			t.Fatalf("心跳失败: %v", err)
		}
		if err := st.Heartbeat(T0 + 3000*sec); err != nil {
			t.Fatalf("心跳失败: %v", err)
		}
	}

	// 基准：一次性跑完。
	st1 := newStore(t)
	ops(st1)
	snap1 := snapshot(t, st1)

	// 原地重建：从事件日志重放。
	if err := st1.Rebuild(); err != nil {
		t.Fatalf("重建失败: %v", err)
	}
	if snap2 := snapshot(t, st1); snap2 != snap1 {
		t.Fatalf("重建后状态不一致:\n--- 原 ---\n%s\n--- 重建 ---\n%s", snap1, snap2)
	}

	// 全新实例重放同一事件流。
	st2 := newStore(t)
	ops(st2)
	if snap3 := snapshot(t, st2); snap3 != snap1 {
		t.Fatalf("新实例重放不一致:\n--- 原 ---\n%s\n--- 新 ---\n%s", snap1, snap3)
	}

	// 进程中断：跑到一半关闭，重开继续。
	halfOps := func(st *Store) {
		setupConfig(t, st)
		b1 := co2Series(1, T0, 20, 5.5, 5.4)
		b1[18].DeviceNs = b1[16].DeviceNs - 30*sec
		dup := smk("co2_primary", 5, T0+40*sec, T0+45*sec, 5.5)
		conflict := smk("co2_primary", 8, T0+70*sec, T0+75*sec, 9.9)
		conflict.FrameID = "co2_primary#8-alt"
		b1 = append(b1, dup, conflict)
		if _, err := st.IngestSamples(b1, "rb-1"); err != nil {
			t.Fatalf("中断前半注入失败: %v", err)
		}
		if _, err := st.IngestSamples(co2Series(21, T0, 20, 5.5, 5.4), "rb-2"); err != nil {
			t.Fatalf("中断前半注入失败: %v", err)
		}
		if err := st.Heartbeat(T0 + 500*sec); err != nil {
			t.Fatalf("心跳失败: %v", err)
		}
		id := mustAlerts(t, st)[0].ID
		if _, err := st.Acknowledge(id, "li-wei", pb.Role_ROLE_FLIGHT_CONTROL, "值班接管", T0+510*sec); err != nil {
			t.Fatalf("确认失败: %v", err)
		}
	}
	restOps := func(st *Store) {
		if _, err := st.IngestSamples(co2Series(41, T0, 40, 2.0, 2.0), "rb-3"); err != nil {
			t.Fatalf("中断后半注入失败: %v", err)
		}
		if _, err := st.IngestSamples(pressureSeries(1, T0+600*sec, 30, 88.0), "rb-4"); err != nil {
			t.Fatalf("中断后半注入失败: %v", err)
		}
		var pressureID string
		for _, a := range mustAlerts(t, st) {
			if a.Rule == "cabin_pressure_low" {
				pressureID = a.ID
			}
		}
		if _, err := st.AppendResolution(pressureID, "dr-chen", pb.Role_ROLE_MEDICAL,
			"舱压回升至 101.2kPa 并复核传感器一致", T0+1100*sec); err != nil {
			t.Fatalf("追加解除证据失败: %v", err)
		}
		if err := st.SetMissionPhase(pb.MissionPhase_MISSION_PHASE_EVA, T0+2000*sec, T0+2000*sec); err != nil {
			t.Fatalf("阶段切换失败: %v", err)
		}
		v2 := core.DefaultThresholdVersion()
		v2.Version = "v2-stricter"
		v2.EffectiveDeviceNs = T0 + 2100*sec
		if err := st.RegisterThresholdVersion(v2, T0+2100*sec); err != nil {
			t.Fatalf("注册 v2 失败: %v", err)
		}
		if _, err := st.IngestSamples(co2Series(100, T0+2200*sec, 40, 5.5, 5.4), "rb-5"); err != nil {
			t.Fatalf("中断后半注入失败: %v", err)
		}
		if err := st.Heartbeat(T0 + 2700*sec); err != nil {
			t.Fatalf("心跳失败: %v", err)
		}
		if err := st.Heartbeat(T0 + 3000*sec); err != nil {
			t.Fatalf("心跳失败: %v", err)
		}
	}

	dbPath := filepath.Join(t.TempDir(), "crash.db")
	st3, err := Open(dbPath)
	if err != nil {
		t.Fatalf("打开失败: %v", err)
	}
	halfOps(st3)
	st3.Close() // 模拟进程中断
	st3, err = Open(dbPath)
	if err != nil {
		t.Fatalf("重开失败: %v", err)
	}
	defer st3.Close()
	restOps(st3)
	if snap4 := snapshot(t, st3); snap4 != snap1 {
		t.Fatalf("进程中断后续跑不一致:\n--- 原 ---\n%s\n--- 中断恢复 ---\n%s", snap1, snap4)
	}
}

// snapshot 导出全部派生状态的规范 JSON（键序固定），用于一致性比较。
func snapshot(t *testing.T, st *Store) string {
	t.Helper()
	type dump struct {
		Alerts      []Alert
		Evidence    map[string][]core.Sample
		Escalations map[string][]Escalation
		Resolutions map[string][]Resolution
		Acks        map[string][]Acknowledgement
		Evaluations []Evaluation
		Health      []SensorState
	}
	d := dump{
		Evidence:    map[string][]core.Sample{},
		Escalations: map[string][]Escalation{},
		Resolutions: map[string][]Resolution{},
		Acks:        map[string][]Acknowledgement{},
	}
	var err error
	if d.Alerts, err = st.ListAlerts(); err != nil {
		t.Fatalf("快照失败: %v", err)
	}
	for _, a := range d.Alerts {
		if d.Evidence[a.ID], err = st.AlertEvidence(a.ID); err != nil {
			t.Fatalf("快照失败: %v", err)
		}
		if d.Escalations[a.ID], err = st.ListEscalations(a.ID); err != nil {
			t.Fatalf("快照失败: %v", err)
		}
		if d.Resolutions[a.ID], err = st.ListResolutions(a.ID); err != nil {
			t.Fatalf("快照失败: %v", err)
		}
		if d.Acks[a.ID], err = st.ListAcknowledgements(a.ID); err != nil {
			t.Fatalf("快照失败: %v", err)
		}
	}
	if d.Evaluations, err = st.ListEvaluations("", 100000); err != nil {
		t.Fatalf("快照失败: %v", err)
	}
	if d.Health, err = st.SensorHealth(); err != nil {
		t.Fatalf("快照失败: %v", err)
	}
	buf, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("快照序列化失败: %v", err)
	}
	return string(buf)
}
