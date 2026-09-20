package core

import (
	"time"

	pb "lifesupport/gen/lifesupport/v1"
)

const (
	sec = int64(time.Second)
	min = int64(time.Minute)
)

// DefaultThresholdVersion 返回在轨常态任务的默认判定配置（版本 v1-on-orbit）。
// 限值量级参考舱段环境控制目标：ppCO2 单位 mmHg，舱压 kPa，风扇转速 rpm。
func DefaultThresholdVersion() *ThresholdVersion {
	return &ThresholdVersion{
		Version:           "v1-on-orbit",
		EffectiveDeviceNs: 0,
		Rules: []ThresholdRule{
			{
				Name:         "co2_high",
				MetricSensor: "co2_primary",
				Unit:         "mmHg",
				Comparison:   pb.Comparison_COMPARISON_ABOVE,
				WarningLimit: 3.0, CriticalLimit: 5.0,
				WarningWindowNs: 10 * min, CriticalWindowNs: 5 * min,
				RecoveryWindowNs: 5 * min, MaxSampleGapNs: 45 * sec,
				Corroborators:       []string{"co2_secondary"},
				CorroboratorStaleNs: 90 * sec,
				SummaryWarning:      "舱内二氧化碳分压持续偏高",
				SummaryCritical:     "舱内二氧化碳分压持续超过红限",
				CrewActionWarning:   "检查 CO2 清除装置运行状态，减少剧烈活动，等待飞控指令",
				CrewActionCritical:  "立即按程序启用备用 CO2 清除通路，佩戴呼吸防护待命，与飞控确认",
			},
			{
				Name:         "cabin_pressure_low",
				MetricSensor: "cabin_pressure",
				Unit:         "kPa",
				Comparison:   pb.Comparison_COMPARISON_BELOW,
				WarningLimit: 96.0, CriticalLimit: 90.0,
				WarningWindowNs: 5 * min, CriticalWindowNs: 2 * min,
				RecoveryWindowNs: 3 * min, MaxSampleGapNs: 45 * sec,
				Corroborators:       []string{"cabin_pressure_backup"},
				CorroboratorStaleNs: 90 * sec,
				SummaryWarning:      "舱压持续低于正常范围",
				SummaryCritical:     "舱压持续低于红限，存在失压风险",
				CrewActionWarning:   "检查舱体密封与供气调压状态，报告异常声响",
				CrewActionCritical:  "立即执行失压应急程序，准备隔离受损舱段并穿戴舱内航天服",
			},
			{
				Name:         "vent_fan_degraded",
				MetricSensor: "fan_a_rpm",
				Unit:         "rpm",
				Comparison:   pb.Comparison_COMPARISON_BELOW,
				WarningLimit: 1200, CriticalLimit: 600,
				WarningWindowNs: 3 * min, CriticalWindowNs: 1 * min,
				RecoveryWindowNs: 2 * min, MaxSampleGapNs: 30 * sec,
				Corroborators:       []string{"co2_secondary"},
				CorroboratorStaleNs: 90 * sec,
				SummaryWarning:      "循环风扇转速持续偏低，舱内通风减弱",
				SummaryCritical:     "循环风扇接近停转，局部 CO2 可能积聚",
				CrewActionWarning:   "避免在通风死角长时间停留，准备切换备用风扇",
				CrewActionCritical:  "立即切换备用循环风扇，远离通风不良区域",
			},
		},
		Policy: EscalationPolicy{
			Critical: []EscalationStep{
				{Level: 1, AfterUnackNs: 2 * min, Notify: "值班飞行总监"},
				{Level: 2, AfterUnackNs: 5 * min, Notify: "飞行总监 + 乘组直接通告"},
				{Level: 3, AfterUnackNs: 10 * min, Notify: "任务指挥中心"},
				{Level: 4, AfterUnresolvedNs: 30 * min, Notify: "任务指挥中心（未解除督办）"},
			},
			Warning: []EscalationStep{
				{Level: 1, AfterUnackNs: 15 * min, Notify: "值班飞行总监"},
				{Level: 2, AfterUnackNs: 60 * min, Notify: "飞行总监"},
				{Level: 3, AfterUnresolvedNs: 120 * min, Notify: "飞行总监（未解除督办）"},
			},
		},
		Overrides: []PhaseOverride{
			// EVA 期间舱内无人活动减少，CO2 黄限放宽；舱压红限更严格。
			{Phase: pb.MissionPhase_MISSION_PHASE_EVA, Rule: "co2_high", WarnLimit: f64(3.5)},
			{Phase: pb.MissionPhase_MISSION_PHASE_EVA, Rule: "cabin_pressure_low", CritLimit: f64(92.0)},
		},
	}
}

func f64(v float64) *float64 { return &v }
