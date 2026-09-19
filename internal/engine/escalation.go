package engine

import (
	lsv1 "lifesupport/gen/lifesupport/v1"
)

// Escalation 计算告警在时刻 now 的升级状态, 是纯函数:
// 级别 1 自 Raised 起算, 每过 IntervalNs 未确认升一级, 确认后冻结。
// 因此升级历史只取决于 (Raised, AckedAt, 策略), 重放必然一致。
func (a *Alert) Escalation(pol EscalationPolicy, now int64) (level int32, history []*lsv1.EscalationStep, nextAt int64) {
	if pol.MaxLevel <= 0 || pol.IntervalNs <= 0 {
		return 0, nil, 0
	}
	// 确认之后升级冻结; 解除自然也不再升级。
	end := now
	if a.Status != lsv1.AlertStatus_ALERT_STATUS_OPEN && a.AckedAt > 0 && a.AckedAt < end {
		end = a.AckedAt
	}
	for k := 1; k <= pol.MaxLevel; k++ {
		at := a.Raised + int64(k-1)*pol.IntervalNs
		if at > end {
			break
		}
		role := ""
		if k-1 < len(pol.Chain) {
			role = pol.Chain[k-1]
		}
		history = append(history, &lsv1.EscalationStep{
			Level:        int32(k),
			AtNs:         at,
			NotifiedRole: role,
		})
		level = int32(k)
	}
	if a.Status == lsv1.AlertStatus_ALERT_STATUS_OPEN && level < int32(pol.MaxLevel) {
		nextAt = a.Raised + int64(level)*pol.IntervalNs
	}
	return level, history, nextAt
}
