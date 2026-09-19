package engine

import (
	"fmt"
	"sort"
	"strings"
	"time"

	lsv1 "lifesupport/gen/lifesupport/v1"
)

// ---------------------------------------------------------------------------
// 视图构建: 由派生状态生成只读视图。所有视图都是 (状态, now) 的纯函数。
// 公开方法持有引擎锁; *Locked 方法假定调用方已持锁。
// ---------------------------------------------------------------------------

// AlertViewByID 返回单条告警视图, 不存在则 nil。
func (e *Engine) AlertViewByID(id string, now int64) *lsv1.AlertView {
	e.mu.Lock()
	defer e.mu.Unlock()
	a := e.alerts[id]
	if a == nil {
		return nil
	}
	return e.alertViewLocked(a, now)
}

// ListAlertViews 按创建顺序返回告警视图。
func (e *Engine) ListAlertViews(includeResolved bool, now int64) []*lsv1.AlertView {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []*lsv1.AlertView
	for _, id := range e.alertIDs {
		a := e.alerts[id]
		if !includeResolved && a.Status == lsv1.AlertStatus_ALERT_STATUS_RESOLVED {
			continue
		}
		out = append(out, e.alertViewLocked(a, now))
	}
	return out
}

// HandoverView 交班视图: 每条告警由哪些采样形成、当前谁接管、距下次升级多久。
func (e *Engine) HandoverView(now int64) *lsv1.HandoverView {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.handoverViewLocked(now)
}

// EnvironmentStatus 返回全部传感器组状态。
func (e *Engine) EnvironmentStatus(now int64) *lsv1.EnvironmentStatusResponse {
	e.mu.Lock()
	defer e.mu.Unlock()
	resp := &lsv1.EnvironmentStatusResponse{ShipTimeNs: e.shipNow}
	for _, gid := range sortedKeys(e.cfg.Groups) {
		resp.Groups = append(resp.Groups, e.groupViewLocked(gid, now))
	}
	return resp
}

func (e *Engine) alertViewLocked(a *Alert, now int64) *lsv1.AlertView {
	pol := e.cfg.Escalation[a.Severity]
	level, history, nextAt := a.Escalation(pol, now)
	v := &lsv1.AlertView{
		AlertId:                 a.ID,
		RuleId:                  a.RuleID,
		SensorId:                a.SensorID,
		GroupId:                 a.GroupID,
		Severity:                a.Severity,
		Status:                  a.Status,
		Summary:                 a.Summary,
		CrewProcedure:           a.CrewProc,
		BreachStartNs:           a.BreachStart,
		BreachEndNs:             a.BreachEnd,
		FormedNs:                a.Formed,
		RaisedNs:                a.Raised,
		ThresholdVersion:        a.Version,
		Phase:                   a.Phase,
		Owner:                   a.Owner,
		OwnerRole:               a.OwnerRole,
		OwnerSinceNs:            a.OwnerSince,
		AckedBy:                 a.AckedBy,
		AckedNs:                 a.AckedAt,
		EscalationLevel:         level,
		NextEscalationNs:        nextAt,
		ResolvedNs:              a.ResolvedAt,
		ResolvedBy:              a.ResolvedBy,
		PeakValue:               a.peak,
		LastValue:               a.last,
		ContributingSampleCount: int32(len(a.samples)),
	}
	if nextAt > now {
		v.NsToNextEscalation = nextAt - now
	}
	for _, s := range a.samples {
		v.ContributingSamples = append(v.ContributingSamples, &lsv1.SampleRef{
			SensorId:     s.SensorId,
			Seq:          s.Seq,
			DeviceTimeNs: s.DeviceTimeNs,
			Value:        s.Value,
		})
	}
	for _, ev := range a.Evidence {
		v.Evidence = append(v.Evidence, &lsv1.EvidenceEntry{
			Operator:     ev.Operator,
			AtNs:         ev.AtNs,
			Summary:      ev.Summary,
			Detail:       ev.Detail,
			IsResolution: ev.IsResolution,
		})
	}
	for _, h := range history {
		v.EscalationHistory = append(v.EscalationHistory, &lsv1.EscalationStep{
			Level:        h.Level,
			AtNs:         h.AtNs,
			NotifiedRole: h.NotifiedRole,
		})
	}
	return v
}

// groupViewLocked 计算关联传感器组状态。
// 任一必需传感器失联 => 证据不足, 绝不据此推导环境安全。
func (e *Engine) groupViewLocked(groupID string, now int64) *lsv1.GroupView {
	sensors := e.cfg.Groups[groupID]
	g := &lsv1.GroupView{GroupId: groupID, Status: lsv1.EnvStatus_ENV_STATUS_NOMINAL}
	for _, sid := range sensors {
		st := e.sensors[sid]
		sv := &lsv1.SensorView{SensorId: sid}
		if st != nil && len(st.samples) > 0 {
			last := st.samples[len(st.samples)-1]
			sv.LastDeviceTimeNs = last.DeviceTimeNs
			sv.LastHeardNs = st.lastHeardNs
			sv.LastValue = last.Value
			sv.ClockOffsetNs = st.offsetEstNs
			sv.DuplicateFrames = st.duplicates
		}
		// 失联判定基于地面收到帧的时刻, 与设备时钟漂移无关。
		if st == nil || now-st.lastHeardNs > e.cfg.SilenceTimeoutNs {
			sv.Silent = true
			g.SilentSensors = append(g.SilentSensors, sid)
		}
		g.Sensors = append(g.Sensors, sv)
	}
	for _, id := range e.alertIDs {
		a := e.alerts[id]
		if a.GroupID == groupID && a.Status != lsv1.AlertStatus_ALERT_STATUS_RESOLVED {
			g.OpenAlertIds = append(g.OpenAlertIds, id)
		}
	}
	switch {
	case len(g.SilentSensors) > 0:
		g.Status = lsv1.EnvStatus_ENV_STATUS_INSUFFICIENT_EVIDENCE
	case len(g.OpenAlertIds) > 0:
		g.Status = lsv1.EnvStatus_ENV_STATUS_ALERTING
	}
	return g
}

func (e *Engine) handoverViewLocked(now int64) *lsv1.HandoverView {
	hv := &lsv1.HandoverView{
		GeneratedAtNs: now,
		ShipTimeNs:    e.shipNow,
	}
	for _, id := range e.alertIDs {
		a := e.alerts[id]
		if a.Status == lsv1.AlertStatus_ALERT_STATUS_RESOLVED {
			continue // 交班关注未决项; 已解除的仍可通过 ListAlerts 查询
		}
		av := e.alertViewLocked(a, now)
		hv.Alerts = append(hv.Alerts, av)
		if a.Status == lsv1.AlertStatus_ALERT_STATUS_OPEN {
			hv.OpenCount++
			if a.Severity == lsv1.Severity_SEVERITY_RED {
				hv.UnackedRedCount++
			}
		}
	}
	// 红色未确认优先, 其余按形成时间。
	sort.SliceStable(hv.Alerts, func(i, j int) bool {
		wi, wj := alertWeight(hv.Alerts[i]), alertWeight(hv.Alerts[j])
		if wi != wj {
			return wi > wj
		}
		return hv.Alerts[i].FormedNs < hv.Alerts[j].FormedNs
	})
	for _, gid := range sortedKeys(e.cfg.Groups) {
		hv.Groups = append(hv.Groups, e.groupViewLocked(gid, now))
	}
	hv.HandoverNotes = e.handoverNotesLocked(hv, now)
	return hv
}

func alertWeight(a *lsv1.AlertView) int {
	w := 0
	if a.Status == lsv1.AlertStatus_ALERT_STATUS_OPEN {
		w += 100
	}
	return w + int(a.Severity)
}

func (e *Engine) handoverNotesLocked(hv *lsv1.HandoverView, now int64) []string {
	var notes []string
	for _, a := range hv.Alerts {
		if a.Status == lsv1.AlertStatus_ALERT_STATUS_OPEN && a.NextEscalationNs > 0 {
			d := time.Duration(max64(0, a.NextEscalationNs-now)) * time.Nanosecond
			notes = append(notes, fmt.Sprintf(
				"%s [%s] 未确认, %s 后升级至级别 %d",
				a.AlertId, a.Severity, d.Truncate(time.Second), a.EscalationLevel+1))
		}
		if a.Status == lsv1.AlertStatus_ALERT_STATUS_ACKED && a.OwnerRole != lsv1.Role_ROLE_UNSPECIFIED {
			notes = append(notes, fmt.Sprintf(
				"%s 已由%s岗位接管确认, 待解除证据", a.AlertId, RoleLabel(a.OwnerRole)))
		}
	}
	for _, g := range hv.Groups {
		if g.Status == lsv1.EnvStatus_ENV_STATUS_INSUFFICIENT_EVIDENCE {
			notes = append(notes, fmt.Sprintf(
				"传感器组 %s 证据不足: %v 失联, 不能据此判断环境安全",
				g.GroupId, g.SilentSensors))
		}
	}
	return notes
}

// RoleLabel 返回角色的中文岗位名 (用于面向各角色通用的提示文本)。
func RoleLabel(r lsv1.Role) string {
	switch r {
	case lsv1.Role_ROLE_CREW:
		return "乘组"
	case lsv1.Role_ROLE_FLIGHT_CONTROL:
		return "飞控"
	case lsv1.Role_ROLE_MEDICAL:
		return "医学支持"
	}
	return "未知"
}

// ---------------------------------------------------------------------------
// 快照: 用于重放一致性校验 (测试与演示)。
// ---------------------------------------------------------------------------

// DumpState 导出全部派生状态 (含已解除告警与传感器统计) 的规范化文本,
// 对 (事件日志, now) 是纯函数。两次运行输出一致即派生状态一致。
func (e *Engine) DumpState(now int64) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "shipNow=%d alerts=%d\n", e.shipNow, len(e.alertIDs))
	for _, id := range e.alertIDs {
		av := e.alertViewLocked(e.alerts[id], now)
		fmt.Fprintf(sb, "alert %s rule=%s sev=%s status=%s breach=[%d,%d] formed=%d raised=%d\n",
			av.AlertId, av.RuleId, av.Severity, av.Status,
			av.BreachStartNs, av.BreachEndNs, av.FormedNs, av.RaisedNs)
		fmt.Fprintf(sb, "  version=%s phase=%s owner=%s(%s)@%d ack=%s@%d samples=%d peak=%.3f\n",
			av.ThresholdVersion, av.Phase, av.Owner, av.OwnerRole, av.OwnerSinceNs,
			av.AckedBy, av.AckedNs, av.ContributingSampleCount, av.PeakValue)
		for _, s := range av.EscalationHistory {
			fmt.Fprintf(sb, "  esc L%d @%d -> %s\n", s.Level, s.AtNs, s.NotifiedRole)
		}
		fmt.Fprintf(sb, "  nextEsc=%d\n", av.NextEscalationNs)
		for _, ev := range av.Evidence {
			fmt.Fprintf(sb, "  evidence by=%s @%d res=%v: %s\n", ev.Operator, ev.AtNs, ev.IsResolution, ev.Summary)
		}
		for _, sr := range av.ContributingSamples {
			fmt.Fprintf(sb, "  sample %s#%d @%d = %.3f\n", sr.SensorId, sr.Seq, sr.DeviceTimeNs, sr.Value)
		}
	}
	for _, gid := range sortedKeys(e.cfg.Groups) {
		g := e.groupViewLocked(gid, now)
		fmt.Fprintf(sb, "group %s status=%s silent=%v open=%v\n", gid, g.Status, g.SilentSensors, g.OpenAlertIds)
		for _, sv := range g.Sensors {
			fmt.Fprintf(sb, "  sensor %s lastDev=%d lastHeard=%d offset=%d dups=%d silent=%v\n",
				sv.SensorId, sv.LastDeviceTimeNs, sv.LastHeardNs, sv.ClockOffsetNs, sv.DuplicateFrames, sv.Silent)
		}
	}
	return sb.String()
}
