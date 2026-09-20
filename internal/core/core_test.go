package core

import (
	"testing"

	pb "lifesupport/gen/lifesupport/v1"
)

func co2Rule() ThresholdRule {
	return ThresholdRule{
		Name:         "co2_high",
		MetricSensor: "co2_primary",
		Comparison:   pb.Comparison_COMPARISON_ABOVE,
		WarningLimit: 3.0, CriticalLimit: 5.0,
		WarningWindowNs: 10 * 60 * sec, CriticalWindowNs: 5 * 60 * sec,
		RecoveryWindowNs: 5 * 60 * sec, MaxSampleGapNs: 45 * sec,
		Corroborators:       []string{"co2_secondary"},
		CorroboratorStaleNs: 90 * sec,
	}
}

// series 生成从 t0 起每 10s 一个采样的序列。
func series(sensor string, t0 int64, values ...float64) []Sample {
	var out []Sample
	for i, v := range values {
		out = append(out, Sample{
			SensorID: sensor, Seq: int64(i + 1), DeviceNs: t0 + int64(i)*10*sec,
			GroundNs: t0 + int64(i)*10*sec, Value: v,
		})
	}
	return out
}

func freshCorroborator(endNs int64) map[string]int64 {
	return map[string]int64{"co2_secondary": endNs - 5*sec}
}

func TestTransientSpikeDoesNotAlert(t *testing.T) {
	rule := co2Rule()
	t0 := int64(1_700_000_000_000_000_000)
	// 正常 - 单点尖峰 - 恢复正常：短暂尖峰不得形成告警。
	values := []float64{2.0, 2.1, 6.5, 2.0, 2.1}
	samples := series("co2_primary", t0, values...)
	end := samples[2].DeviceNs
	res := Evaluate(rule, end, samples[:3], freshCorroborator(end))
	if res.Verdict != pb.Verdict_VERDICT_TRANSIENT {
		t.Fatalf("单点尖峰应判定 TRANSIENT，得到 %v (%s)", res.Verdict, res.Detail)
	}
	// 尖峰过后恢复正常。
	end = samples[4].DeviceNs
	res = Evaluate(rule, end, samples, freshCorroborator(end))
	if res.Verdict != pb.Verdict_VERDICT_NOMINAL {
		t.Fatalf("尖峰过后应判定 NOMINAL，得到 %v (%s)", res.Verdict, res.Detail)
	}
}

func TestSustainedBreachOpensCritical(t *testing.T) {
	rule := co2Rule()
	t0 := int64(1_700_000_000_000_000_000)
	// 31 个采样 × 10s = 300s 持续越红限。
	var values []float64
	for i := 0; i < 31; i++ {
		values = append(values, 5.5)
	}
	samples := series("co2_primary", t0, values...)
	end := samples[30].DeviceNs
	res := Evaluate(rule, end, samples, freshCorroborator(end))
	if res.Verdict != pb.Verdict_VERDICT_SUSTAINED_BREACH || res.Severity != pb.Severity_SEVERITY_CRITICAL {
		t.Fatalf("持续越红限 300s 应判定红警，得到 %v/%v (%s)", res.Verdict, res.Severity, res.Detail)
	}
	if res.RunStartNs != t0 {
		t.Fatalf("越限段起点应为 %d，得到 %d", t0, res.RunStartNs)
	}
	if len(res.Evidence) != 31 {
		t.Fatalf("证据应包含 31 个采样，得到 %d", len(res.Evidence))
	}
	// 少一个采样（290s）不得越线。
	res = Evaluate(rule, samples[29].DeviceNs, samples[:30], freshCorroborator(samples[29].DeviceNs))
	if res.Verdict == pb.Verdict_VERDICT_SUSTAINED_BREACH {
		t.Fatalf("290s 未达 300s 窗口，不应形成红警")
	}
}

func TestMissingCorroboratorIsInsufficientEvidence(t *testing.T) {
	rule := co2Rule()
	t0 := int64(1_700_000_000_000_000_000)
	var values []float64
	for i := 0; i < 40; i++ {
		values = append(values, 5.5)
	}
	samples := series("co2_primary", t0, values...)
	end := samples[39].DeviceNs

	// 关联传感器完全缺失。
	res := Evaluate(rule, end, samples, map[string]int64{})
	if res.Verdict != pb.Verdict_VERDICT_INSUFFICIENT_EVIDENCE {
		t.Fatalf("关联传感器缺失应判定证据不足，得到 %v", res.Verdict)
	}
	if len(res.Missing) != 1 || res.Missing[0] != "co2_secondary" {
		t.Fatalf("缺失列表应为 [co2_secondary]，得到 %v", res.Missing)
	}

	// 关联传感器滞后超过允许值同样证据不足。
	res = Evaluate(rule, end, samples, map[string]int64{"co2_secondary": end - 300*sec})
	if res.Verdict != pb.Verdict_VERDICT_INSUFFICIENT_EVIDENCE {
		t.Fatalf("关联传感器滞后就判定证据不足，得到 %v", res.Verdict)
	}
}

func TestNoSamplesIsInsufficientNotSafe(t *testing.T) {
	rule := co2Rule()
	res := Evaluate(rule, 1_700_000_000_000_000_000, nil, freshCorroborator(1_700_000_000_000_000_000))
	if res.Verdict != pb.Verdict_VERDICT_INSUFFICIENT_EVIDENCE {
		t.Fatalf("主传感器无数据只能判定证据不足，得到 %v", res.Verdict)
	}
}

func TestStalePrimaryIsInsufficient(t *testing.T) {
	rule := co2Rule()
	t0 := int64(1_700_000_000_000_000_000)
	samples := series("co2_primary", t0, 5.5, 5.5, 5.5)
	// 判定时刻远晚于最后一个主传感器采样（主传感器静默）。
	end := t0 + 10*60*sec
	res := Evaluate(rule, end, samples, freshCorroborator(end))
	if res.Verdict != pb.Verdict_VERDICT_INSUFFICIENT_EVIDENCE {
		t.Fatalf("主传感器滞后应判定证据不足，得到 %v (%s)", res.Verdict, res.Detail)
	}
}

func TestGapBreaksEvidenceChain(t *testing.T) {
	rule := co2Rule()
	t0 := int64(1_700_000_000_000_000_000)
	// 两段各 200s 的越限，中间隔 100s 空窗（超过 45s 允许间隔）。
	var samples []Sample
	samples = append(samples, series("co2_primary", t0, repeat(5.5, 20)...)...)
	tail := series("co2_primary", t0+300*sec, repeat(5.5, 20)...)
	for i := range tail {
		tail[i].Seq = int64(21 + i)
	}
	samples = append(samples, tail...)
	end := samples[len(samples)-1].DeviceNs
	res := Evaluate(rule, end, samples, freshCorroborator(end))
	if res.Verdict == pb.Verdict_VERDICT_SUSTAINED_BREACH {
		t.Fatalf("证据链被空窗中断，不得把两段拼成持续越限")
	}
}

func TestClockDriftDoesNotBreakRun(t *testing.T) {
	rule := co2Rule()
	t0 := int64(1_700_000_000_000_000_000)
	samples := series("co2_primary", t0, repeat(5.5, 35)...)
	// 中间一次设备时钟回退（漂移）：序列仍连续。
	samples[15].DeviceNs = samples[14].DeviceNs - 20*sec
	end := samples[34].DeviceNs
	res := Evaluate(rule, end, samples, freshCorroborator(end))
	if res.Verdict != pb.Verdict_VERDICT_SUSTAINED_BREACH {
		t.Fatalf("时钟漂移不应中断连续越限段，得到 %v (%s)", res.Verdict, res.Detail)
	}
}

func TestBadQualityBreaksRun(t *testing.T) {
	rule := co2Rule()
	t0 := int64(1_700_000_000_000_000_000)
	samples := series("co2_primary", t0, repeat(5.5, 35)...)
	samples[10].Quality = 1 // 坏值
	end := samples[34].DeviceNs
	res := Evaluate(rule, end, samples, freshCorroborator(end))
	// 坏值后只剩 24 个好采样（230s），不足 300s 红警窗口。
	if res.Verdict == pb.Verdict_VERDICT_SUSTAINED_BREACH {
		t.Fatalf("坏值中断证据链后不应凑满红警窗口")
	}
}

func TestEscalationSchedule(t *testing.T) {
	policy := EscalationPolicy{
		Critical: []EscalationStep{
			{Level: 1, AfterUnackNs: 2 * 60 * sec},
			{Level: 2, AfterUnackNs: 5 * 60 * sec},
			{Level: 4, AfterUnresolvedNs: 30 * 60 * sec},
		},
	}
	g0 := int64(1_700_000_000_000_000_000)
	dues := EscalationSchedule(policy, pb.Severity_SEVERITY_CRITICAL, pb.AlertState_ALERT_STATE_OPEN, g0, 0, 0)
	if len(dues) != 2 || dues[0].DueNs != g0+2*60*sec || dues[1].DueNs != g0+5*60*sec {
		t.Fatalf("未确认红警升级表错误: %+v", dues)
	}
	// 已确认：只有未解除路径的步骤。
	ackAt := g0 + 6*60*sec
	dues = EscalationSchedule(policy, pb.Severity_SEVERITY_CRITICAL, pb.AlertState_ALERT_STATE_ACKNOWLEDGED, g0, ackAt, 2)
	if len(dues) != 1 || dues[0].Level != 4 || dues[0].DueNs != ackAt+30*60*sec {
		t.Fatalf("已确认未解除升级表错误: %+v", dues)
	}
}

func TestPhaseOverride(t *testing.T) {
	tv := DefaultThresholdVersion()
	rule, ok := tv.RuleFor("co2_high", pb.MissionPhase_MISSION_PHASE_EVA)
	if !ok {
		t.Fatal("规则应存在")
	}
	if rule.WarningLimit != 3.5 {
		t.Fatalf("EVA 阶段 CO2 黄限应覆盖为 3.5，得到 %v", rule.WarningLimit)
	}
	if rule.CriticalLimit != 5.0 {
		t.Fatalf("EVA 阶段 CO2 红限不应被覆盖，得到 %v", rule.CriticalLimit)
	}
	rule, _ = tv.RuleFor("co2_high", pb.MissionPhase_MISSION_PHASE_ON_ORBIT)
	if rule.WarningLimit != 3.0 {
		t.Fatalf("在轨阶段黄限应为默认 3.0，得到 %v", rule.WarningLimit)
	}
}

func repeat(v float64, n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = v
	}
	return out
}
