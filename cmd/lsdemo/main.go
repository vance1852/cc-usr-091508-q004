// lsdemo 用一段包含时钟漂移、重复帧、进程中断与离线补传的脚本化事件流
// 演示生命保障告警模块:
//
//  1. 短暂尖峰不形成告警, 持续超限才形成;
//  2. 传感器失联 => 证据不足, 不能推导环境安全;
//  3. 离线补传按原始采样序列归并, 可补出持续窗口;
//  4. 进程中断后重放日志, 未决告警/升级历史/确认人稳定重建;
//  5. 交班视图按乘组/飞控/医学三种角色裁剪。
//
// 运行: go run ./cmd/lsdemo
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	lsv1 "lifesupport/gen/lifesupport/v1"
	"lifesupport/internal/engine"
	"lifesupport/internal/server"
)

const (
	sec = int64(time.Second)
	G0  = int64(1_700_000_000) * sec // 地面时刻基准
)

// clock 手动地面时钟, 让演示完全确定。
type clock struct{ t int64 }

func (c *clock) now() int64  { return c.t }
func (c *clock) set(t int64) { c.t = t }

// dev 把地面时刻映射为设备采样时刻: 固定偏差 2s, 并随时间漂移 (1ms/s)。
func dev(ground int64) int64 {
	return ground - 2*sec + (ground-G0)/1_000_000
}

type scenario struct {
	eng *engine.Engine
	clk *clock
}

// feed 在地面时刻 g 送入一批采样。
func (s *scenario) feed(g int64, samples ...*lsv1.Sample) {
	s.clk.set(g)
	for _, sm := range samples {
		sm.GroundRecvNs = g
	}
	if _, err := s.eng.Apply(engine.EvSampleBatch, &lsv1.SampleBatchEvent{Samples: samples}, "", g); err != nil {
		panic(err)
	}
}

type step struct {
	at int64 // 地面时刻
	fn func()
}

// buildSteps 构建完整脚本; 返回的步骤可分段执行以模拟进程中断。
func buildSteps(s *scenario) []step {
	at := func(offsetSec int64) int64 { return G0 + offsetSec*sec }
	seq := map[string]int64{}
	mk := func(g int64, sensor string, v float64) *lsv1.Sample {
		seq[sensor]++
		return &lsv1.Sample{SensorId: sensor, Seq: seq[sensor], DeviceTimeNs: dev(g), GroundRecvNs: g, Value: v}
	}
	frame := func(g int64, co2, cabin, fan float64) []*lsv1.Sample {
		return []*lsv1.Sample{mk(g, "co2_pp", co2), mk(g, "cabin_pressure", cabin), mk(g, "fan_speed", fan)}
	}
	normal := func(g int64) []*lsv1.Sample { return frame(g, 1.5, 101.3, 1200) }

	var steps []step
	add := func(offsetSec int64, fn func()) { steps = append(steps, step{at(offsetSec), fn}) }

	// 播种: 阈值版本 v1 + 在轨阶段
	add(0, func() {
		must2(s.eng.Apply(engine.EvThresholds, engine.DefaultThresholds(0), "seed:thr", at(0)))
		must2(s.eng.Apply(engine.EvPhase, &lsv1.PhaseChangedEvent{
			Phase: lsv1.MissionPhase_MISSION_PHASE_ON_ORBIT, SetBy: "fco-seed",
		}, "seed:phase", at(0)))
	})
	// 1-60. 正常遥测 60s
	for i := int64(1); i <= 60; i++ {
		g := at(i)
		add(i, func() { s.feed(g, normal(g)...) })
	}
	// 61-63. CO2 短暂尖峰 5.2 (3s < 60s 窗口) —— 不应形成告警
	for i := int64(61); i <= 63; i++ {
		g := at(i)
		add(i, func() { s.feed(g, frame(g, 5.2, 101.3, 1200)...) })
	}
	// 64. 检查点: 尖峰过后应无告警
	add(64, func() {
		hv := s.eng.HandoverView(at(64))
		fmt.Printf("[t+64s] CO2 尖峰 3s 后: 未决告警 %d 条 (尖峰被持续窗口过滤)\n", hv.OpenCount)
	})
	// 64-160. CO2 持续 4.5: t=124 应形成红色告警
	for i := int64(64); i <= 160; i++ {
		g := at(i)
		add(i, func() { s.feed(g, frame(g, 4.5, 101.3, 1200)...) })
	}
	// 161. 地面链路重传 70-72 的重复帧 —— 应被去重
	add(161, func() {
		var dups []*lsv1.Sample
		for _, g := range []int64{at(70), at(71), at(72)} {
			dups = append(dups, &lsv1.Sample{SensorId: "co2_pp", Seq: (g - G0) / sec, DeviceTimeNs: dev(g), GroundRecvNs: at(161), Value: 4.5})
		}
		s.feed(at(161), dups...)
	})
	// 162. 检查点
	add(162, func() {
		hv := s.eng.HandoverView(at(162))
		fmt.Printf("[t+162s] 未决告警 %d 条 (红色未确认 %d)\n", hv.OpenCount, hv.UnackedRedCount)
	})
	// 170. 飞控确认 CO2 告警
	add(170, func() {
		id := firstPendingAlert(s.eng, at(170))
		must2(s.eng.Apply(engine.EvAck, &lsv1.AckEvent{
			AlertId: id, Operator: "fco-zhang", Role: lsv1.Role_ROLE_FLIGHT_CONTROL, Note: "已通知乘组执行 LS-01",
		}, "", at(170)))
	})
	// 175. 交班: 接管人变更为 fco-li
	add(175, func() {
		id := firstPendingAlert(s.eng, at(175))
		must2(s.eng.Apply(engine.EvAssign, &lsv1.AssignEvent{
			AlertId: id, Operator: "fco-li", Role: lsv1.Role_ROLE_FLIGHT_CONTROL, Note: "轨道日交班",
		}, "", at(175)))
	})
	// 176-180. CO2 回落至正常
	for i := int64(176); i <= 180; i++ {
		g := at(i)
		add(i, func() { s.feed(g, normal(g)...) })
	}
	// 181-235. 风扇遥测中断 (失联 55s); 其余传感器正常
	for i := int64(181); i <= 235; i++ {
		g := at(i)
		add(i, func() {
			s.feed(g, mk(g, "co2_pp", 1.5), mk(g, "cabin_pressure", 101.3))
		})
	}
	// 220. 检查点: 风扇失联 40s > 30s 超时 => 证据不足
	add(220, func() {
		env := s.eng.EnvironmentStatus(at(220))
		for _, g := range env.Groups {
			fmt.Printf("[t+220s] 传感器组 %s 状态=%s 失联=%v\n", g.GroupId, g.Status, g.SilentSensors)
		}
	})
	// 236. 离线补传: 风扇 181-235 的帧 (转速 500, 低于红线) 按原始序号归并
	add(236, func() {
		var bf []*lsv1.Sample
		for i := int64(181); i <= 235; i++ {
			g := at(i)
			bf = append(bf, &lsv1.Sample{SensorId: "fan_speed", Seq: (g - G0) / sec, DeviceTimeNs: dev(g), GroundRecvNs: at(236), Value: 500})
		}
		seq["fan_speed"] = 235 // 补传占用了 181-235 的序号
		s.feed(at(236), bf...)
		fmt.Printf("[t+236s] 离线补传 %d 帧风扇数据\n", len(bf))
	})
	// 237-240. 风扇恢复实时遥测 (仍偏低)
	for i := int64(237); i <= 240; i++ {
		g := at(i)
		add(i, func() { s.feed(g, frame(g, 1.5, 101.3, 500)...) })
	}
	// 240. 解除 CO2 告警 (追加解除证据)
	add(240, func() {
		for _, av := range s.eng.ListAlertViews(true, at(240)) {
			if av.RuleId == "co2-red" && av.Status == lsv1.AlertStatus_ALERT_STATUS_ACKED {
				must2(s.eng.Apply(engine.EvResolve, &lsv1.ResolveEvent{
					AlertId: av.AlertId, Operator: "fco-li",
					Summary: "已更换 CO2 清除罐, 分压连续 30 分钟低于注意阈值",
					Detail:  "备件编号 SC-221, 医学支持确认乘组无高碳酸血症症状",
				}, "", at(240)))
			}
		}
	})
	// 250. 启用阈值版本 v2 (CO2 注意阈值收紧到 1.8)
	add(250, func() {
		v2 := engine.DefaultThresholds(dev(at(250)))
		v2.Version = "v2-stricter-co2"
		for _, r := range v2.Rules {
			if r.RuleId == "co2-caution" {
				r.Threshold = 1.8
			}
		}
		must2(s.eng.Apply(engine.EvThresholds, v2, "seed:thr2", at(250)))
	})
	// 251-380. CO2 维持 1.9: v1 下不越限, v2 下 120s 后形成注意告警
	for i := int64(251); i <= 380; i++ {
		g := at(i)
		add(i, func() { s.feed(g, frame(g, 1.9, 101.3, 1150)...) })
	}
	// 381. 终态检查点
	add(381, func() {
		hv := s.eng.HandoverView(at(381))
		fmt.Printf("[t+381s] 未决告警 %d 条; 升级历史与确认人见交班视图\n", hv.OpenCount)
	})
	return steps
}

func runSteps(s *scenario, steps []step, from, to int) {
	for i := from; i < to && i < len(steps); i++ {
		s.clk.set(steps[i].at)
		steps[i].fn()
	}
}

func main() {
	keep := flag.Bool("keep", false, "保留演示数据库 (默认运行后删除)")
	flag.Parse()
	dir, err := os.MkdirTemp("", "lsdemo-*")
	if err != nil {
		panic(err)
	}
	if !*keep {
		defer os.RemoveAll(dir)
	}
	fmt.Printf("演示数据库目录: %s\n\n", dir)

	// ---- 运行 A: 一次性跑完整事件流 -----------------------------------------
	clkA := &clock{t: G0}
	engA, err := engine.Open(filepath.Join(dir, "run-a.db"), engine.DefaultConfig(clkA.now))
	must(err)
	sA := &scenario{eng: engA, clk: clkA}
	stepsA := buildSteps(sA)
	runSteps(sA, stepsA, 0, len(stepsA))
	dumpA := engA.DumpState(clkA.now())

	// ---- 运行 B: 中途"进程中断", 重放日志后续传 -------------------------------
	clkB := &clock{t: G0}
	dbB := filepath.Join(dir, "run-b.db")
	engB, err := engine.Open(dbB, engine.DefaultConfig(clkB.now))
	must(err)
	sB := &scenario{eng: engB, clk: clkB}
	stepsB := buildSteps(sB)
	cut := 130 // 在第 130 步后中断 (CO2 告警确认前后)
	runSteps(sB, stepsB, 0, cut)
	must(engB.Close())
	fmt.Println("—— 进程中断, 重新打开数据库并重放事件日志 ——")
	engB2, err := engine.Open(dbB, engine.DefaultConfig(clkB.now))
	must(err)
	sB.eng = engB2
	runSteps(sB, stepsB, cut, len(stepsB))
	dumpB := engB2.DumpState(clkB.now())

	fmt.Println()
	if dumpA == dumpB {
		fmt.Println("✔ 重放一致性: 中断续传与一次跑完的派生状态完全相同")
	} else {
		fmt.Println("✘ 重放结果不一致!")
		fmt.Println("---- run A ----\n", dumpA)
		fmt.Println("---- run B ----\n", dumpB)
		os.Exit(1)
	}
	fmt.Println()

	// ---- 交班视图: 三种角色 --------------------------------------------------
	for _, role := range []lsv1.Role{
		lsv1.Role_ROLE_FLIGHT_CONTROL,
		lsv1.Role_ROLE_CREW,
		lsv1.Role_ROLE_MEDICAL,
	} {
		printHandover(server.MaskHandoverFor(role, engA.HandoverView(clkA.now())), engine.RoleLabel(role))
	}
	engA.Close()
	engB2.Close()
}

func firstPendingAlert(eng *engine.Engine, now int64) string {
	for _, av := range eng.ListAlertViews(true, now) {
		if av.Status == lsv1.AlertStatus_ALERT_STATUS_OPEN || av.Status == lsv1.AlertStatus_ALERT_STATUS_ACKED {
			return av.AlertId
		}
	}
	panic("no pending alert")
}

func printHandover(hv *lsv1.HandoverView, who string) {
	fmt.Printf("================ 交班视图 [%s] ================\n", who)
	fmt.Printf("未决告警: %d, 红色未确认: %d\n", hv.OpenCount, hv.UnackedRedCount)
	for _, a := range hv.Alerts {
		fmt.Printf("  %s [%s/%s] %s\n", a.AlertId, a.Severity, a.Status, a.Summary)
		fmt.Printf("    越限区间(设备时钟): %d .. %d, 形成于 %d\n", a.BreachStartNs, a.BreachEndNs, a.FormedNs)
		fmt.Printf("    形成采样: %d 条 (本角色可见明细 %d 条)\n", a.ContributingSampleCount, len(a.ContributingSamples))
		if a.Owner != "" || a.OwnerRole != lsv1.Role_ROLE_UNSPECIFIED {
			fmt.Printf("    当前接管: %s (%s)\n", a.Owner, a.OwnerRole)
		}
		if a.NextEscalationNs > 0 {
			fmt.Printf("    距下次升级: %ds (当前级别 %d)\n", a.NsToNextEscalation/sec, a.EscalationLevel)
		}
		if a.CrewProcedure != "" {
			fmt.Printf("    乘组程序: %s\n", a.CrewProcedure)
		}
		if a.ThresholdVersion != "" {
			fmt.Printf("    阈值版本: %s, 任务阶段: %s\n", a.ThresholdVersion, a.Phase)
		}
	}
	for _, g := range hv.Groups {
		fmt.Printf("  传感器组 %s: %s (失联: %v)\n", g.GroupId, g.Status, g.SilentSensors)
	}
	for _, n := range hv.HandoverNotes {
		fmt.Printf("  ★ %s\n", n)
	}
	fmt.Println()
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func must2(_ interface{}, err error) {
	if err != nil {
		panic(err)
	}
}
