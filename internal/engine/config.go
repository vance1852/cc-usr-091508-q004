package engine

import (
	"time"

	lsv1 "lifesupport/gen/lifesupport/v1"
)

// DefaultConfig 返回演示与测试共用的默认策略配置。
func DefaultConfig(now func() int64) Config {
	return Config{
		Groups: map[string][]string{
			"atmosphere": {"co2_pp", "cabin_pressure", "fan_speed"},
		},
		SilenceTimeoutNs: int64(30 * time.Second),
		Escalation: map[lsv1.Severity]EscalationPolicy{
			lsv1.Severity_SEVERITY_RED: {
				IntervalNs: int64(5 * time.Minute),
				MaxLevel:   3,
				Chain:      []string{"值班飞控", "值班主任", "飞行总监"},
			},
			lsv1.Severity_SEVERITY_CAUTION: {
				IntervalNs: int64(15 * time.Minute),
				MaxLevel:   2,
				Chain:      []string{"值班飞控", "值班主任"},
			},
			lsv1.Severity_SEVERITY_ADVISORY: {
				IntervalNs: int64(time.Hour),
				MaxLevel:   1,
				Chain:      []string{"值班飞控"},
			},
		},
		Now: now,
	}
}

// DefaultThresholds 返回在轨阶段默认阈值规则 (版本 "v1-onorbit")。
// 设备时钟基准由调用方给出 (effectiveFrom)。
func DefaultThresholds(effectiveFrom int64) *lsv1.ThresholdsActivatedEvent {
	rule := func(id, sensor, summary, proc string, dir lsv1.RuleDirection,
		thr float64, window, gap time.Duration, sev lsv1.Severity,
		phases ...lsv1.MissionPhase) *lsv1.RuleSpec {
		return &lsv1.RuleSpec{
			RuleId:        id,
			SensorId:      sensor,
			GroupId:       "atmosphere",
			Direction:     dir,
			Threshold:     thr,
			WindowNs:      int64(window),
			MaxGapNs:      int64(gap),
			Severity:      sev,
			Phases:        phases,
			Summary:       summary,
			CrewProcedure: proc,
		}
	}
	return &lsv1.ThresholdsActivatedEvent{
		Version:               "v1-onorbit",
		EffectiveFromDeviceNs: effectiveFrom,
		ActivatedBy:           "system-seed",
		Note:                  "在轨默认生命保障阈值",
		Rules: []*lsv1.RuleSpec{
			rule("co2-caution", "co2_pp", "CO2 分压偏高", "LS-02 检查 CO2 清除装置",
				lsv1.RuleDirection_RULE_DIRECTION_ABOVE, 2.0, 120*time.Second, 5*time.Second,
				lsv1.Severity_SEVERITY_CAUTION),
			rule("co2-red", "co2_pp", "CO2 分压持续超限", "LS-01 立即佩戴呼吸防护并启动备用清除",
				lsv1.RuleDirection_RULE_DIRECTION_ABOVE, 4.0, 60*time.Second, 5*time.Second,
				lsv1.Severity_SEVERITY_RED),
			// 睡眠阶段 CO2 红色阈值更严格: 演示任务阶段参与判定。
			rule("co2-red-sleep", "co2_pp", "睡眠阶段 CO2 分压超限", "LS-01 立即佩戴呼吸防护并启动备用清除",
				lsv1.RuleDirection_RULE_DIRECTION_ABOVE, 3.0, 60*time.Second, 5*time.Second,
				lsv1.Severity_SEVERITY_RED, lsv1.MissionPhase_MISSION_PHASE_SLEEP),
			rule("cabin-low-red", "cabin_pressure", "舱压持续偏低", "LS-10 检漏并准备应急供氧",
				lsv1.RuleDirection_RULE_DIRECTION_BELOW, 70.0, 30*time.Second, 5*time.Second,
				lsv1.Severity_SEVERITY_RED),
			rule("cabin-high-caution", "cabin_pressure", "舱压偏高", "LS-11 检查调压阀",
				lsv1.RuleDirection_RULE_DIRECTION_ABOVE, 103.5, 180*time.Second, 5*time.Second,
				lsv1.Severity_SEVERITY_CAUTION),
			rule("fan-low-red", "fan_speed", "循环风扇转速过低", "LS-20 切换备用风扇",
				lsv1.RuleDirection_RULE_DIRECTION_BELOW, 800.0, 45*time.Second, 5*time.Second,
				lsv1.Severity_SEVERITY_RED),
		},
	}
}
