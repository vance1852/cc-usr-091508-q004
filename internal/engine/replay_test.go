package engine

import (
	"fmt"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	lsv1 "lifesupport/gen/lifesupport/v1"
)

// streamEvent 是脚本化事件流中的一条事件。
type streamEvent struct {
	typ string
	msg proto.Message
	uid string
	at  int64 // 地面到达时刻
}

// dev 设备时钟: 相对地面基准每秒钟漂移 +2ms (模拟星上钟漂)。
func dev(offSec int64) int64 { return D0 + offSec*sec + offSec*2_000_000 }

// gnd 地面时刻: 设备时刻滞后约 2s。
func gnd(offSec int64) int64 { return D0 + offSec*sec + 2*sec }

// buildStream 构建包含时钟漂移、重复帧、离线补传、确认/接管/解除、
// 阈值版本切换与任务阶段切换的完整事件流。
func buildStream() []streamEvent {
	var evs []streamEvent
	add := func(typ string, msg proto.Message, uid string, at int64) {
		evs = append(evs, streamEvent{typ, msg, uid, at})
	}
	smp := func(sensor string, offSec int64, v float64) *lsv1.Sample {
		return &lsv1.Sample{
			SensorId:     sensor,
			Seq:          offSec,
			DeviceTimeNs: dev(offSec),
			GroundRecvNs: gnd(offSec),
			Value:        v,
		}
	}
	// batch 生成 [from,to) 秒的三传感器帧 (fan 可缺席以模拟失联)。
	batch := func(from, to int64, co2, cabin, fan float64, withFan bool) *lsv1.SampleBatchEvent {
		b := &lsv1.SampleBatchEvent{}
		for i := from; i < to; i++ {
			b.Samples = append(b.Samples, smp("co2_pp", i, co2), smp("cabin_pressure", i, cabin))
			if withFan {
				b.Samples = append(b.Samples, smp("fan_speed", i, fan))
			}
		}
		return b
	}

	add(EvThresholds, DefaultThresholds(0), "seed:thr", gnd(0))
	add(EvPhase, &lsv1.PhaseChangedEvent{
		Phase: lsv1.MissionPhase_MISSION_PHASE_ON_ORBIT, SetBy: "fco-seed",
	}, "seed:phase", gnd(0))

	// 0-60s 正常遥测 (10s 一批)。
	for base := int64(0); base < 60; base += 10 {
		add(EvSampleBatch, batch(base, base+10, 1.5, 101.3, 1200, true), "", gnd(base+10))
	}
	// 61-63s CO2 尖峰 (不形成告警)。
	add(EvSampleBatch, batch(61, 64, 5.2, 101.3, 1200, true), "", gnd(64))
	// 64-160s CO2 持续 4.5 (红色告警, 10s 一批)。
	for base := int64(64); base < 160; base += 10 {
		add(EvSampleBatch, batch(base, base+10, 4.5, 101.3, 1200, true), "", gnd(base+10))
	}
	// 重复帧: 重传 70-79 并附新帧 160。
	dup := batch(70, 80, 4.5, 101.3, 1200, true)
	dup.Samples = append(dup.Samples, smp("co2_pp", 160, 4.5))
	add(EvSampleBatch, dup, "", gnd(161))
	// 确认 + 交班接管。
	add(EvAck, &lsv1.AckEvent{AlertId: "ALR-0001", Operator: "fco-zhang", Role: lsv1.Role_ROLE_FLIGHT_CONTROL}, "ack:1", gnd(170))
	add(EvAssign, &lsv1.AssignEvent{AlertId: "ALR-0001", Operator: "fco-li", Role: lsv1.Role_ROLE_FLIGHT_CONTROL, Note: "轨道日交班"}, "assign:1", gnd(175))
	// 176-180s CO2 回落。
	add(EvSampleBatch, batch(161, 181, 1.5, 101.3, 1200, true), "", gnd(181))
	// 181-235s 风扇失联 (帧中无 fan_speed)。
	for base := int64(181); base < 235; base += 18 {
		add(EvSampleBatch, batch(base, base+18, 1.5, 101.3, 0, false), "", gnd(base+18))
	}
	// 离线补传风扇 181-235 (转速 500)。
	bf := &lsv1.SampleBatchEvent{}
	for i := int64(181); i < 236; i++ {
		bf.Samples = append(bf.Samples, smp("fan_speed", i, 500))
	}
	add(EvSampleBatch, bf, "", gnd(236))
	// 236-240s 风扇恢复实时 (仍偏低) => 补出红色告警。
	add(EvSampleBatch, batch(236, 241, 1.5, 101.3, 500, true), "", gnd(241))
	// 解除 CO2 告警 (追加解除证据)。
	add(EvResolve, &lsv1.ResolveEvent{
		AlertId: "ALR-0001", Operator: "fco-li",
		Summary: "已更换 CO2 清除罐, 分压连续 30 分钟低于注意阈值",
	}, "resolve:1", gnd(245))
	// 阈值版本 v2 (CO2 注意阈值 1.8) 自 dev(250) 生效。
	v2 := DefaultThresholds(dev(250))
	v2.Version = "v2-stricter"
	for _, r := range v2.Rules {
		if r.RuleId == "co2-caution" {
			r.Threshold = 1.8
		}
	}
	add(EvThresholds, v2, "seed:thr2", gnd(250))
	// 睡眠阶段自 dev(300) 起。
	add(EvPhase, &lsv1.PhaseChangedEvent{
		Phase: lsv1.MissionPhase_MISSION_PHASE_SLEEP, EffectiveFromDeviceNs: dev(300), SetBy: "fco-plan",
	}, "phase:sleep", gnd(300))
	// 251-380s CO2 维持 1.9 (v2 下 120s 后形成注意告警)。
	for base := int64(251); base < 380; base += 20 {
		add(EvSampleBatch, batch(base, base+20, 1.9, 101.3, 1150, true), "", gnd(base+20))
	}
	// 幂等重试: 重复确认事件 (同幂等键, 应被安全忽略)。
	add(EvAck, &lsv1.AckEvent{AlertId: "ALR-0001", Operator: "fco-zhang", Role: lsv1.Role_ROLE_FLIGHT_CONTROL}, "ack:1", gnd(390))
	return evs
}

// runStream 在 dbPath 上执行事件流; cut >= 0 时在第 cut 条前关闭并重开
// (模拟进程中断), 返回终态快照。
func runStream(t *testing.T, dbPath string, evs []streamEvent, cut int, now int64) string {
	t.Helper()
	eng, err := Open(dbPath, DefaultConfig(nil))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i, ev := range evs {
		if i == cut {
			if err := eng.Close(); err != nil {
				t.Fatalf("close at cut: %v", err)
			}
			eng, err = Open(dbPath, DefaultConfig(nil))
			if err != nil {
				t.Fatalf("reopen at cut %d: %v", cut, err)
			}
		}
		if _, err := eng.Apply(ev.typ, ev.msg, ev.uid, ev.at); err != nil {
			t.Fatalf("event %d (%s): %v", i, ev.typ, err)
		}
	}
	dump := eng.DumpState(now)
	eng.Close()
	return dump
}

// TestReplayDeterminismEverySplitPoint 在事件流的每一个可能中断点上
// 模拟进程崩溃-重启, 终态必须与一次跑完完全一致。
func TestReplayDeterminismEverySplitPoint(t *testing.T) {
	evs := buildStream()
	now := gnd(400) + 3600*sec // 流结束后一小时, 让升级充分累积
	dir := t.TempDir()

	ref := runStream(t, filepath.Join(dir, "ref.db"), evs, -1, now)

	for cut := 0; cut <= len(evs); cut++ {
		got := runStream(t, filepath.Join(dir, fmt.Sprintf("cut-%03d.db", cut)), evs, cut, now)
		if got != ref {
			t.Fatalf("cut=%d 的重放结果与连续运行不一致\n--- ref ---\n%s\n--- cut %d ---\n%s", cut, ref, cut, got)
		}
	}
}

// TestReplayFromLogOnly 全部事件入日志后, 单纯重放日志也必须得到相同状态。
func TestReplayFromLogOnly(t *testing.T) {
	evs := buildStream()
	now := gnd(400) + 3600*sec
	dir := t.TempDir()
	db := filepath.Join(dir, "full.db")

	live := runStream(t, db, evs, -1, now)

	// 只重放: 不再送入任何新事件。
	eng, err := Open(db, DefaultConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	replayed := eng.DumpState(now)
	eng.Close()
	if live != replayed {
		t.Fatalf("日志重放与在线运行不一致\n--- live ---\n%s\n--- replay ---\n%s", live, replayed)
	}
}

// TestReplayContents 对参考运行做内容级断言, 防止"一致但错误"。
func TestReplayContents(t *testing.T) {
	evs := buildStream()
	now := gnd(400) + 3600*sec
	db := filepath.Join(t.TempDir(), "content.db")
	eng, err := Open(db, DefaultConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	for i, ev := range evs {
		if _, err := eng.Apply(ev.typ, ev.msg, ev.uid, ev.at); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}

	views := eng.ListAlertViews(true, now)
	if len(views) != 3 {
		t.Fatalf("alerts = %d, want 3 (co2-red, fan-low-red, co2-caution@v2)", len(views))
	}
	byRule := map[string]*lsv1.AlertView{}
	for _, v := range views {
		byRule[v.RuleId] = v
	}
	// CO2 红色: 已解除, 确认人 fco-zhang, 接管人 fco-li, 证据含解除项。
	co2 := byRule["co2-red"]
	if co2 == nil || co2.Status != lsv1.AlertStatus_ALERT_STATUS_RESOLVED {
		t.Fatalf("co2-red: %+v", co2)
	}
	if co2.AckedBy != "fco-zhang" || co2.Owner != "fco-li" || co2.ResolvedBy != "fco-li" {
		t.Fatalf("co2-red people: ack=%s owner=%s resolved=%s", co2.AckedBy, co2.Owner, co2.ResolvedBy)
	}
	if len(co2.Evidence) != 1 || !co2.Evidence[0].IsResolution {
		t.Fatalf("co2-red evidence: %v", co2.Evidence)
	}
	// 风扇红色: 由离线补传形成, 未确认, 一小时后应升到 3 级。
	fan := byRule["fan-low-red"]
	if fan == nil || fan.Status != lsv1.AlertStatus_ALERT_STATUS_OPEN {
		t.Fatalf("fan-low-red: %+v", fan)
	}
	if fan.EscalationLevel != 3 || len(fan.EscalationHistory) != 3 {
		t.Fatalf("fan escalation: level=%d history=%v", fan.EscalationLevel, fan.EscalationHistory)
	}
	if fan.ThresholdVersion != "v1-onorbit" {
		t.Fatalf("fan version = %s", fan.ThresholdVersion)
	}
	// v2 注意告警: 必须由 v2 阈值判定。
	caution := byRule["co2-caution"]
	if caution == nil || caution.ThresholdVersion != "v2-stricter" {
		t.Fatalf("co2-caution: %+v", caution)
	}
	// 传感器组: 所有传感器在线, 有未决告警 => ALERTING。
	env := eng.EnvironmentStatus(gnd(395))
	if env.Groups[0].Status != lsv1.EnvStatus_ENV_STATUS_ALERTING {
		t.Fatalf("group status = %s", env.Groups[0].Status)
	}
}
