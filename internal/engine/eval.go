package engine

import (
	"fmt"
	"sort"

	lsv1 "lifesupport/gen/lifesupport/v1"
)

// ---------------------------------------------------------------------------
// 遥测批次: 去重、按原始采样序列归并、按设备时钟评估持续窗口。
// ---------------------------------------------------------------------------

func (e *Engine) applySampleBatch(b *lsv1.SampleBatchEvent, arrivalNs int64, res *ApplyResult) {
	// 批次内先按 (传感器, 设备时刻, 序号) 排序, 保证告警创建顺序与到达顺序无关,
	// 只取决于事件日志中的位置 —— 这是重放确定性的前提。
	samples := make([]*lsv1.Sample, len(b.Samples))
	copy(samples, b.Samples)
	sort.Slice(samples, func(i, j int) bool {
		a, c := samples[i], samples[j]
		if a.SensorId != c.SensorId {
			return a.SensorId < c.SensorId
		}
		if a.DeviceTimeNs != c.DeviceTimeNs {
			return a.DeviceTimeNs < c.DeviceTimeNs
		}
		return a.Seq < c.Seq
	})

	touched := map[string]bool{}
	for _, s := range samples {
		st := e.sensors[s.SensorId]
		if st == nil {
			st = &sensorState{seenSeq: map[int64]struct{}{}}
			e.sensors[s.SensorId] = st
		}
		// 重复帧: 同一传感器同一星上序号只计一次。
		if _, dup := st.seenSeq[s.Seq]; dup {
			st.duplicates++
			res.Duplicates++
			continue
		}
		st.seenSeq[s.Seq] = struct{}{}

		// 按设备采样时钟归并 (离线补传的旧帧插入到正确位置)。
		idx := sort.Search(len(st.samples), func(i int) bool {
			cur := st.samples[i]
			if cur.DeviceTimeNs != s.DeviceTimeNs {
				return cur.DeviceTimeNs > s.DeviceTimeNs
			}
			return cur.Seq >= s.Seq
		})
		cp := &lsv1.Sample{
			SensorId:     s.SensorId,
			Seq:          s.Seq,
			DeviceTimeNs: s.DeviceTimeNs,
			GroundRecvNs: s.GroundRecvNs,
			Value:        s.Value,
		}
		st.samples = append(st.samples, nil)
		copy(st.samples[idx+1:], st.samples[idx:])
		st.samples[idx] = cp

		// 地面-设备钟差估计 (EWMA), 仅用于展示, 不参与判定。
		off := s.GroundRecvNs - s.DeviceTimeNs
		if !st.offsetInit {
			st.offsetEstNs, st.offsetInit = off, true
		} else {
			st.offsetEstNs = st.offsetEstNs*7/8 + off/8
		}
		if s.GroundRecvNs > st.lastHeardNs {
			st.lastHeardNs = s.GroundRecvNs
		}
		if arrivalNs > st.lastHeardNs {
			st.lastHeardNs = arrivalNs
		}
		if s.DeviceTimeNs > e.shipNow {
			e.shipNow = s.DeviceTimeNs
		}
		res.Accepted++
		touched[s.SensorId] = true
	}

	// 对本次涉及的传感器重新评估持续窗口。
	for _, id := range sortedKeys(touched) {
		e.evaluateSensor(id, arrivalNs, res)
	}
}

// evaluateSensor 对单个传感器重算全部规则版本的越限区间, 并与既有告警归并。
func (e *Engine) evaluateSensor(sensorID string, arrivalNs int64, res *ApplyResult) {
	st := e.sensors[sensorID]
	if st == nil {
		return
	}
	for vi := range e.versions {
		v := &e.versions[vi]
		var end int64 = 1<<62 - 1
		if vi+1 < len(e.versions) {
			end = e.versions[vi+1].effectiveFrom
		}
		for _, r := range v.rules {
			if r.SensorId != sensorID {
				continue
			}
			e.reconcileRule(v, r, st.samples, end, arrivalNs, res)
		}
	}
}

// breachInterval 是一段连续越限区间 (设备时钟域)。
type breachInterval struct {
	start, end int64
	idxs       []int // 参与采样的下标 (在传感器已排序样本中)
}

// findBreaches 在版本生效区间 [vStart, vEnd) 内扫描持续越限区间。
// 采样是否越限由该采样设备时刻对应的规则决定; 不适用本规则的任务阶段
// 与超过 max_gap 的数据空洞都会中断区间。
func (e *Engine) findBreaches(samples []*lsv1.Sample, r *lsv1.RuleSpec, vStart, vEnd int64) []breachInterval {
	var out []breachInterval
	var cur *breachInterval
	var prevDev int64

	closeCur := func() {
		if cur != nil {
			out = append(out, *cur)
			cur = nil
		}
	}
	for i := range samples {
		s := samples[i]
		if s.DeviceTimeNs < vStart || s.DeviceTimeNs >= vEnd {
			continue
		}
		if !ruleAppliesToPhase(r, e.phaseAt(s.DeviceTimeNs)) || !violates(r, s.Value) {
			closeCur()
			prevDev = 0
			continue
		}
		if cur == nil || s.DeviceTimeNs-prevDev > r.MaxGapNs {
			closeCur()
			cur = &breachInterval{start: s.DeviceTimeNs}
		}
		cur.end = s.DeviceTimeNs
		cur.idxs = append(cur.idxs, i)
		prevDev = s.DeviceTimeNs
	}
	closeCur()
	return out
}

func ruleAppliesToPhase(r *lsv1.RuleSpec, ph lsv1.MissionPhase) bool {
	if len(r.Phases) == 0 {
		return true
	}
	for _, p := range r.Phases {
		if p == ph {
			return true
		}
	}
	return false
}

func violates(r *lsv1.RuleSpec, v float64) bool {
	switch r.Direction {
	case lsv1.RuleDirection_RULE_DIRECTION_ABOVE:
		return v > r.Threshold
	case lsv1.RuleDirection_RULE_DIRECTION_BELOW:
		return v < r.Threshold
	}
	return false
}

// reconcileRule 将持续时长达标的区间归并到告警登记表:
// 与既有告警区间重叠者并入该告警 (保留其形成时刻与结论),
// 否则创建新告警。告警一经创建永不删除。
func (e *Engine) reconcileRule(v *thresholdVersion, r *lsv1.RuleSpec, samples []*lsv1.Sample, vEnd, arrivalNs int64, res *ApplyResult) {
	for _, iv := range e.findBreaches(samples, r, v.effectiveFrom, vEnd) {
		if iv.end-iv.start < r.WindowNs {
			continue // 短暂尖峰, 未达持续窗口
		}
		a := e.findOverlappingAlert(v.version, r.RuleId, iv)
		if a == nil {
			a = e.newAlert(v, r, iv, arrivalNs)
			res.NewAlertIDs = append(res.NewAlertIDs, a.ID)
		}
		a.BreachStart = min64(a.BreachStart, iv.start)
		a.BreachEnd = max64(a.BreachEnd, iv.end)
		for _, i := range iv.idxs {
			s := samples[i]
			if _, ok := a.sampleSeq[s.Seq]; ok {
				continue
			}
			a.sampleSeq[s.Seq] = struct{}{}
			a.samples = append(a.samples, &lsv1.SampleRef{
				SensorId:     s.SensorId,
				Seq:          s.Seq,
				DeviceTimeNs: s.DeviceTimeNs,
				Value:        s.Value,
			})
			if s.Value > a.peak {
				a.peak = s.Value
			}
			a.last = s.Value
		}
	}
}

// findOverlappingAlert 在同一 (版本, 规则) 下寻找区间重叠的未决/已决告警。
func (e *Engine) findOverlappingAlert(version, ruleID string, iv breachInterval) *Alert {
	for _, id := range e.alertIDs {
		a := e.alerts[id]
		if a.RuleID != ruleID || a.Version != version {
			continue
		}
		if iv.start <= a.BreachEnd && a.BreachStart <= iv.end {
			return a
		}
	}
	return nil
}

func (e *Engine) newAlert(v *thresholdVersion, r *lsv1.RuleSpec, iv breachInterval, arrivalNs int64) *Alert {
	e.alertSeq++
	a := &Alert{
		ID:          fmt.Sprintf("ALR-%04d", e.alertSeq),
		RuleID:      r.RuleId,
		SensorID:    r.SensorId,
		GroupID:     r.GroupId,
		Severity:    r.Severity,
		Status:      lsv1.AlertStatus_ALERT_STATUS_OPEN,
		Summary:     r.Summary,
		CrewProc:    r.CrewProcedure,
		BreachStart: iv.start,
		BreachEnd:   iv.end,
		Formed:      iv.start + r.WindowNs, // 窗口首次满足的设备时刻
		Raised:      arrivalNs,
		Version:     v.version,
		Phase:       e.phaseAt(iv.start),
		sampleSeq:   map[int64]struct{}{},
		peak:        -1e300,
	}
	e.alerts[a.ID] = a
	e.alertIDs = append(e.alertIDs, a.ID)
	return a
}

// ---------------------------------------------------------------------------
// 阈值版本 / 任务阶段事件
// ---------------------------------------------------------------------------

func (e *Engine) applyThresholds(t *lsv1.ThresholdsActivatedEvent, arrivalNs int64) {
	e.versions = append(e.versions, thresholdVersion{
		version:       t.Version,
		effectiveFrom: t.EffectiveFromDeviceNs,
		rules:         t.Rules,
	})
	// 新版本的生效区间可能覆盖已收到的采样 (例如先传数后启用阈值),
	// 需要对所有传感器在新版本区间内补做一次评估。
	for _, id := range sortedKeys(e.sensors) {
		e.evaluateSensor(id, arrivalNs, &ApplyResult{})
	}
}

func (e *Engine) applyPhase(p *lsv1.PhaseChangedEvent, arrivalNs int64) {
	e.phases = append(e.phases, phaseMark{
		effectiveFrom: p.EffectiveFromDeviceNs,
		phase:         p.Phase,
	})
	// 阶段适用性变化同样可能影响既有采样的判定。
	for _, id := range sortedKeys(e.sensors) {
		e.evaluateSensor(id, arrivalNs, &ApplyResult{})
	}
}

// ---------------------------------------------------------------------------
// 告警处置: 确认 / 接管 / 追加证据 / 解除 (全部只追加, 不删除)
// ---------------------------------------------------------------------------

func (e *Engine) applyAck(a *lsv1.AckEvent, arrivalNs int64) {
	al := e.alerts[a.AlertId]
	al.Status = lsv1.AlertStatus_ALERT_STATUS_ACKED
	al.AckedBy = a.Operator
	al.AckedAt = arrivalNs
	al.Owner = a.Operator
	al.OwnerRole = a.Role
	al.OwnerSince = arrivalNs
}

func (e *Engine) applyAssign(a *lsv1.AssignEvent, arrivalNs int64) {
	al := e.alerts[a.AlertId]
	al.Owner = a.Operator
	al.OwnerRole = a.Role
	al.OwnerSince = arrivalNs
}

func (e *Engine) applyEvidence(v *lsv1.EvidenceEvent, arrivalNs int64) {
	al := e.alerts[v.AlertId]
	al.Evidence = append(al.Evidence, &lsv1.EvidenceEntry{
		Operator: v.Operator,
		AtNs:     arrivalNs,
		Summary:  v.Summary,
		Detail:   v.Detail,
	})
}

func (e *Engine) applyResolve(r *lsv1.ResolveEvent, arrivalNs int64) {
	al := e.alerts[r.AlertId]
	al.Evidence = append(al.Evidence, &lsv1.EvidenceEntry{
		Operator:     r.Operator,
		AtNs:         arrivalNs,
		Summary:      r.Summary,
		Detail:       r.Detail,
		IsResolution: true,
	})
	al.Status = lsv1.AlertStatus_ALERT_STATUS_RESOLVED
	al.ResolvedAt = arrivalNs
	al.ResolvedBy = r.Operator
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
