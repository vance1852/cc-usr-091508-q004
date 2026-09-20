// Package core 实现生命保障告警的纯函数判定引擎。
//
// 设计要点：
//   - 持续窗口按设备采样时钟判定，升级时限按地面接收时刻推进；
//   - 判定同时参考任务阶段与阈值版本，结论一旦记录不可改写；
//   - 单个传感器失联只能得到“证据不足”，绝不据此推断环境安全；
//   - 全部函数无副作用、无系统时钟读取，相同输入序列必得相同结果，
//     因此包含时钟漂移、重复帧与进程中断的事件流可以稳定重放。
package core

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	pb "lifesupport/gen/lifesupport/v1"
)

// Sample 是一条归一化后的遥测采样，时间均为 UTC 纳秒。
type Sample struct {
	SensorID string
	Seq      int64 // 设备侧原始采样序列
	FrameID  string
	DeviceNs int64 // 设备采样时钟
	GroundNs int64 // 地面接收时刻
	Value    float64
	Unit     string
	Quality  int32 // 0 = 有效
}

// ThresholdRule 描述单条告警规则。
type ThresholdRule struct {
	Name                string
	MetricSensor        string
	Unit                string
	Comparison          pb.Comparison
	WarningLimit        float64
	CriticalLimit       float64
	WarningWindowNs     int64 // 黄警持续窗口（设备时钟）
	CriticalWindowNs    int64 // 红警持续窗口（设备时钟）
	RecoveryWindowNs    int64 // 恢复正常持续窗口
	MaxSampleGapNs      int64 // 超过此间隔视为证据中断
	Corroborators       []string
	CorroboratorStaleNs int64
	SummaryWarning      string
	SummaryCritical     string
	CrewActionWarning   string
	CrewActionCritical  string
}

// Breaches 报告取值是否越过给定严重度的限值。
func (r ThresholdRule) Breaches(value float64, sev pb.Severity) bool {
	limit := r.limitFor(sev)
	if r.Comparison == pb.Comparison_COMPARISON_BELOW {
		return value < limit
	}
	return value > limit
}

func (r ThresholdRule) limitFor(sev pb.Severity) float64 {
	if sev == pb.Severity_SEVERITY_CRITICAL {
		return r.CriticalLimit
	}
	return r.WarningLimit
}

// LimitFor 暴露限值查询（交班视图展示“越过了哪条限值”）。
func (r ThresholdRule) LimitFor(sev pb.Severity) float64 { return r.limitFor(sev) }

// EscalationStep 是一级升级。
type EscalationStep struct {
	Level             int32
	AfterUnackNs      int64 // 未确认：自告警开启（地面时刻）起
	AfterUnresolvedNs int64 // 已确认未解除：自确认（地面时刻）起
	Notify            string
}

// EscalationPolicy 按严重度给出升级链。
type EscalationPolicy struct {
	Critical []EscalationStep
	Warning  []EscalationStep
}

// Steps 返回对应严重度的升级链（按 level 升序）。
func (p EscalationPolicy) Steps(sev pb.Severity) []EscalationStep {
	if sev == pb.Severity_SEVERITY_CRITICAL {
		return p.Critical
	}
	return p.Warning
}

// PhaseOverride 是任务阶段对限值的覆盖。
type PhaseOverride struct {
	Phase     pb.MissionPhase
	Rule      string
	WarnLimit *float64
	CritLimit *float64
}

// ThresholdVersion 是一套带版本的判定配置。
type ThresholdVersion struct {
	Version           string
	EffectiveDeviceNs int64
	Rules             []ThresholdRule
	Policy            EscalationPolicy
	Overrides         []PhaseOverride
}

// RuleFor 返回应用任务阶段覆盖后的规则。
func (tv *ThresholdVersion) RuleFor(name string, phase pb.MissionPhase) (ThresholdRule, bool) {
	var rule ThresholdRule
	found := false
	for _, r := range tv.Rules {
		if r.Name == name {
			rule = r
			found = true
			break
		}
	}
	if !found {
		return ThresholdRule{}, false
	}
	for _, ov := range tv.Overrides {
		if ov.Rule == name && ov.Phase == phase {
			if ov.WarnLimit != nil {
				rule.WarningLimit = *ov.WarnLimit
			}
			if ov.CritLimit != nil {
				rule.CriticalLimit = *ov.CritLimit
			}
		}
	}
	return rule, true
}

// RuleNames 返回全部规则名（排序保证确定性）。
func (tv *ThresholdVersion) RuleNames() []string {
	names := make([]string, 0, len(tv.Rules))
	for _, r := range tv.Rules {
		names = append(names, r.Name)
	}
	sort.Strings(names)
	return names
}

// RulesForSensor 返回与该传感器相关的规则名（主指标或关联传感器）。
func (tv *ThresholdVersion) RulesForSensor(sensorID string) []string {
	var names []string
	for _, r := range tv.Rules {
		if r.MetricSensor == sensorID {
			names = append(names, r.Name)
			continue
		}
		for _, c := range r.Corroborators {
			if c == sensorID {
				names = append(names, r.Name)
				break
			}
		}
	}
	sort.Strings(names)
	return names
}

// ---------------------------------------------------------------------------
// 判定
// ---------------------------------------------------------------------------

// EvalResult 是一次判定的结论。
type EvalResult struct {
	Rule           string
	Verdict        pb.Verdict
	Severity       pb.Severity
	EndDeviceNs    int64
	RunStartNs     int64    // 当前越限段起点（设备时钟），无越限时为 0
	Evidence       []Sample // 形成结论的采样（越限段或短暂尖峰）
	NominalSinceNs int64    // 末尾持续正常段的起点（设备时钟），无则为 0
	Missing        []string
	Detail         string
}

// Evaluate 在设备时刻 endNs 对规则做一次判定。
//
// samples 为主传感器按原始采样序列升序、device_ns <= endNs 的有效采样；
// corroboratorLast 给出各关联传感器在 endNs 之前最后一次采样的设备时刻。
// 函数不读取任何时钟，结果只取决于输入。
func Evaluate(rule ThresholdRule, endNs int64, samples []Sample, corroboratorLast map[string]int64) EvalResult {
	res := EvalResult{Rule: rule.Name, Verdict: pb.Verdict_VERDICT_NOMINAL, EndDeviceNs: endNs}

	if len(samples) == 0 {
		res.Verdict = pb.Verdict_VERDICT_INSUFFICIENT_EVIDENCE
		res.Detail = fmt.Sprintf("主传感器 %s 在判定窗口前无任何采样，证据不足；失联不能推断环境安全", rule.MetricSensor)
		return res
	}

	// 主传感器在判定时刻必须仍然在线：最后一个有效采样不得滞后超过允许间隔。
	lastPrimary := int64(-1)
	for i := len(samples) - 1; i >= 0; i-- {
		if samples[i].Quality == 0 {
			lastPrimary = samples[i].DeviceNs
			break
		}
	}
	if lastPrimary < 0 || (rule.MaxSampleGapNs > 0 && endNs-lastPrimary > rule.MaxSampleGapNs) {
		res.Verdict = pb.Verdict_VERDICT_INSUFFICIENT_EVIDENCE
		res.Detail = fmt.Sprintf("主传感器 %s 数据滞后（最后有效采样距判定时刻超过允许间隔），证据不足；失联不能推断环境安全", rule.MetricSensor)
		return res
	}

	// 关联传感器必须在窗口末端附近仍然在线。
	for _, c := range rule.Corroborators {
		last, ok := corroboratorLast[c]
		if !ok || endNs-last > rule.CorroboratorStaleNs {
			res.Missing = append(res.Missing, c)
		}
	}
	if len(res.Missing) > 0 {
		sort.Strings(res.Missing)
		res.Verdict = pb.Verdict_VERDICT_INSUFFICIENT_EVIDENCE
		res.Detail = fmt.Sprintf("关联传感器 %s 缺失或滞后超过 %s，证据不足；不能推断环境安全",
			strings.Join(res.Missing, ","), fmtNs(rule.CorroboratorStaleNs))
		return res
	}

	// 扫描越限段与正常段。坏值（quality != 0）中断证据链。
	// 设备时钟回退（漂移）用单调包络校正：回退不制造伪间隔，
	// 真实的采样静默（校正后仍超过允许间隔）才会中断证据链。
	corr := make([]int64, len(samples))
	for i := range samples {
		if i == 0 || samples[i].DeviceNs > corr[i-1] {
			corr[i] = samples[i].DeviceNs
		} else {
			corr[i] = corr[i-1]
		}
	}
	var (
		runStart     = -1 // 当前越限段在 samples 中的起始下标
		runSev       = pb.Severity_SEVERITY_UNSPECIFIED
		nominalSince int64
		lastGood     = -1
	)
	type runInfo struct {
		startNs int64
		sev     pb.Severity
		startIx int
		endIx   int
	}
	var lastRun *runInfo

	closeRun := func(endIx int) {
		if runStart >= 0 && endIx >= runStart {
			lastRun = &runInfo{startNs: samples[runStart].DeviceNs, sev: runSev, startIx: runStart, endIx: endIx}
		}
		runStart = -1
		runSev = pb.Severity_SEVERITY_UNSPECIFIED
	}

	nominalSince = 0
	for i, s := range samples {
		if s.Quality != 0 {
			closeRun(lastGood)
			nominalSince = 0
			lastGood = -1
			continue
		}
		if lastGood >= 0 && rule.MaxSampleGapNs > 0 {
			gap := corr[i] - corr[lastGood]
			if gap > rule.MaxSampleGapNs {
				// 证据中断：越限段与正常段都到此为止。
				closeRun(lastGood)
				nominalSince = s.DeviceNs
			}
		}
		warn := rule.Breaches(s.Value, pb.Severity_SEVERITY_WARNING)
		crit := rule.Breaches(s.Value, pb.Severity_SEVERITY_CRITICAL)
		switch {
		case crit || warn:
			sev := pb.Severity_SEVERITY_WARNING
			if crit {
				sev = pb.Severity_SEVERITY_CRITICAL
			}
			if runStart < 0 {
				runStart = i
				runSev = sev
			} else if sev == pb.Severity_SEVERITY_CRITICAL {
				runSev = sev
			}
			nominalSince = 0
		default:
			closeRun(lastGood)
			if nominalSince == 0 {
				nominalSince = s.DeviceNs
			}
		}
		lastGood = i
	}
	closeRun(len(samples) - 1)

	// 末尾存在持续正常段。
	if nominalSince != 0 {
		res.NominalSinceNs = nominalSince
	}

	// 越限段必须延伸到最后一个有效采样（越限仍在继续）。
	lastIdx := len(samples) - 1
	for lastIdx >= 0 && samples[lastIdx].Quality != 0 {
		lastIdx--
	}
	if lastRun == nil || lastRun.endIx != lastIdx {
		if res.NominalSinceNs != 0 {
			res.Detail = fmt.Sprintf("窗口末端指标正常（自 %s 起持续正常）", fmtTs(res.NominalSinceNs))
		} else {
			res.Detail = "窗口内未见越限"
		}
		return res
	}

	run := lastRun
	res.RunStartNs = run.startNs
	res.Evidence = append([]Sample(nil), samples[run.startIx:run.endIx+1]...)
	runDur := corr[lastIdx] - corr[run.startIx]

	// 红警窗口要求段内每个采样都越红限；黄警窗口要求都越黄限。
	critDur := trailingBreachDuration(rule, samples, corr, lastIdx, pb.Severity_SEVERITY_CRITICAL)
	warnDur := trailingBreachDuration(rule, samples, corr, lastIdx, pb.Severity_SEVERITY_WARNING)

	switch {
	case critDur >= rule.CriticalWindowNs && rule.CriticalWindowNs >= 0:
		res.Verdict = pb.Verdict_VERDICT_SUSTAINED_BREACH
		res.Severity = pb.Severity_SEVERITY_CRITICAL
		res.Detail = fmt.Sprintf("持续越红限 %.4g 已达 %s（窗口要求 %s，%d 个采样）",
			rule.CriticalLimit, fmtNs(critDur), fmtNs(rule.CriticalWindowNs), len(res.Evidence))
	case warnDur >= rule.WarningWindowNs && rule.WarningWindowNs >= 0:
		res.Verdict = pb.Verdict_VERDICT_SUSTAINED_BREACH
		res.Severity = pb.Severity_SEVERITY_WARNING
		res.Detail = fmt.Sprintf("持续越黄限 %.4g 已达 %s（窗口要求 %s，%d 个采样）",
			rule.WarningLimit, fmtNs(warnDur), fmtNs(rule.WarningWindowNs), len(res.Evidence))
	default:
		res.Verdict = pb.Verdict_VERDICT_TRANSIENT
		res.Severity = run.sev
		res.Detail = fmt.Sprintf("越限持续 %s 未达窗口（黄 %s / 红 %s），判定为短暂尖峰",
			fmtNs(runDur), fmtNs(rule.WarningWindowNs), fmtNs(rule.CriticalWindowNs))
	}
	return res
}

// trailingBreachDuration 计算截至 lastIdx、每个采样都越过 sev 限值的
// 连续段时长（corr 为漂移校正后的设备时钟；坏值/间隔中断该段）。
func trailingBreachDuration(rule ThresholdRule, samples []Sample, corr []int64, lastIdx int, sev pb.Severity) int64 {
	if lastIdx < 0 {
		return 0
	}
	start := lastIdx
	for i := lastIdx; i >= 0; i-- {
		s := samples[i]
		if s.Quality != 0 || !rule.Breaches(s.Value, sev) {
			break
		}
		if i < lastIdx && rule.MaxSampleGapNs > 0 {
			if corr[i+1]-corr[i] > rule.MaxSampleGapNs {
				break
			}
		}
		start = i
	}
	d := corr[lastIdx] - corr[start]
	if d < 0 {
		return 0
	}
	return d
}

// ---------------------------------------------------------------------------
// 暴露统计（医学视图）
// ---------------------------------------------------------------------------

// ExposureStats 由证据采样计算暴露统计：峰值、均值、越限持续时长。
// 持续时长按相邻同传感器采样的设备时钟差积分（时钟回退钳为 0）。
func ExposureStats(samples []Sample, rule ThresholdRule) (maxV, meanV float64, warnDur, critDur int64) {
	if len(samples) == 0 {
		return 0, 0, 0, 0
	}
	maxV = samples[0].Value
	var sum float64
	for i, sm := range samples {
		if sm.Value > maxV {
			maxV = sm.Value
		}
		sum += sm.Value
		if i+1 >= len(samples) {
			break
		}
		next := samples[i+1]
		if next.SensorID != sm.SensorID {
			continue
		}
		dt := next.DeviceNs - sm.DeviceNs
		if dt < 0 {
			dt = 0
		}
		if rule.Breaches(sm.Value, pb.Severity_SEVERITY_WARNING) {
			warnDur += dt
		}
		if rule.Breaches(sm.Value, pb.Severity_SEVERITY_CRITICAL) {
			critDur += dt
		}
	}
	return maxV, sum / float64(len(samples)), warnDur, critDur
}

// AlertID 由规则与越限段起点导出确定性告警标识。
func AlertID(rule string, runStartNs int64) string {
	return rule + "-" + strconv.FormatInt(runStartNs, 36)
}

// ---------------------------------------------------------------------------
// 升级计算
// ---------------------------------------------------------------------------

// EscalationDue 是一级应升级的确定性时刻。
type EscalationDue struct {
	Level  int32
	DueNs  int64
	Notify string
}

// EscalationSchedule 计算告警自当前状态起的升级时刻表。
// state 为 OPEN 时使用 openedGroundNs 为基准；ACKNOWLEDGED 时使用 ackGroundNs。
// 返回全部 level > currentLevel 的应升级时刻（升序）。
func EscalationSchedule(policy EscalationPolicy, sev pb.Severity, state pb.AlertState,
	openedGroundNs, ackGroundNs int64, currentLevel int32) []EscalationDue {
	var dues []EscalationDue
	for _, st := range policy.Steps(sev) {
		if st.Level <= currentLevel {
			continue
		}
		var due int64 = -1
		switch state {
		case pb.AlertState_ALERT_STATE_OPEN:
			if st.AfterUnackNs > 0 {
				due = openedGroundNs + st.AfterUnackNs
			}
		case pb.AlertState_ALERT_STATE_ACKNOWLEDGED:
			if st.AfterUnresolvedNs > 0 {
				due = ackGroundNs + st.AfterUnresolvedNs
			}
		}
		if due >= 0 {
			dues = append(dues, EscalationDue{Level: st.Level, DueNs: due, Notify: st.Notify})
		}
	}
	sort.Slice(dues, func(i, j int) bool {
		if dues[i].DueNs != dues[j].DueNs {
			return dues[i].DueNs < dues[j].DueNs
		}
		return dues[i].Level < dues[j].Level
	})
	return dues
}

// ---------------------------------------------------------------------------
// 配置 <-> proto 转换
// ---------------------------------------------------------------------------

// ConfigFromProto 转换并校验阈值版本配置。
func ConfigFromProto(cfg *pb.ThresholdVersionConfig) (*ThresholdVersion, error) {
	if cfg == nil {
		return nil, fmt.Errorf("配置为空")
	}
	if strings.TrimSpace(cfg.Version) == "" {
		return nil, fmt.Errorf("阈值版本号为空")
	}
	tv := &ThresholdVersion{Version: cfg.Version}
	if cfg.EffectiveFromDeviceTime != nil {
		tv.EffectiveDeviceNs = cfg.EffectiveFromDeviceTime.AsTime().UnixNano()
	}
	seen := map[string]bool{}
	for _, r := range cfg.Rules {
		rule, err := ruleFromProto(r)
		if err != nil {
			return nil, fmt.Errorf("规则 %q: %w", r.Name, err)
		}
		if seen[rule.Name] {
			return nil, fmt.Errorf("规则 %q 重复", rule.Name)
		}
		seen[rule.Name] = true
		tv.Rules = append(tv.Rules, rule)
	}
	if len(tv.Rules) == 0 {
		return nil, fmt.Errorf("至少需要一个规则")
	}
	tv.Policy.Critical = stepsFromProto(cfg.Escalation.GetCriticalSteps())
	tv.Policy.Warning = stepsFromProto(cfg.Escalation.GetWarningSteps())
	if err := validateSteps(tv.Policy.Critical); err != nil {
		return nil, fmt.Errorf("红警升级链: %w", err)
	}
	if err := validateSteps(tv.Policy.Warning); err != nil {
		return nil, fmt.Errorf("黄警升级链: %w", err)
	}
	for _, ov := range cfg.PhaseOverrides {
		if !seen[ov.Rule] {
			return nil, fmt.Errorf("阶段覆盖引用了不存在的规则 %q", ov.Rule)
		}
		po := PhaseOverride{Phase: ov.Phase, Rule: ov.Rule}
		if ov.WarningLimit != nil {
			v := ov.GetWarningLimit()
			po.WarnLimit = &v
		}
		if ov.CriticalLimit != nil {
			v := ov.GetCriticalLimit()
			po.CritLimit = &v
		}
		tv.Overrides = append(tv.Overrides, po)
	}
	return tv, nil
}

func ruleFromProto(r *pb.ThresholdRule) (ThresholdRule, error) {
	if r.Name == "" || r.MetricSensor == "" {
		return ThresholdRule{}, fmt.Errorf("规则名与主传感器不能为空")
	}
	if r.Comparison != pb.Comparison_COMPARISON_ABOVE && r.Comparison != pb.Comparison_COMPARISON_BELOW {
		return ThresholdRule{}, fmt.Errorf("必须指定比较方向")
	}
	rule := ThresholdRule{
		Name:               r.Name,
		MetricSensor:       r.MetricSensor,
		Unit:               r.Unit,
		Comparison:         r.Comparison,
		WarningLimit:       r.WarningLimit,
		CriticalLimit:      r.CriticalLimit,
		Corroborators:      append([]string(nil), r.Corroborators...),
		SummaryWarning:     r.SummaryWarning,
		SummaryCritical:    r.SummaryCritical,
		CrewActionWarning:  r.CrewActionWarning,
		CrewActionCritical: r.CrewActionCritical,
	}
	if r.WarningWindow != nil {
		rule.WarningWindowNs = r.WarningWindow.AsDuration().Nanoseconds()
	}
	if r.CriticalWindow != nil {
		rule.CriticalWindowNs = r.CriticalWindow.AsDuration().Nanoseconds()
	}
	if r.RecoveryWindow != nil {
		rule.RecoveryWindowNs = r.RecoveryWindow.AsDuration().Nanoseconds()
	}
	if r.MaxSampleGap != nil {
		rule.MaxSampleGapNs = r.MaxSampleGap.AsDuration().Nanoseconds()
	}
	if r.CorroboratorStaleness != nil {
		rule.CorroboratorStaleNs = r.CorroboratorStaleness.AsDuration().Nanoseconds()
	}
	if len(rule.Corroborators) > 0 && rule.CorroboratorStaleNs <= 0 {
		return ThresholdRule{}, fmt.Errorf("配置了关联传感器时必须给出允许滞后")
	}
	return rule, nil
}

func stepsFromProto(steps []*pb.EscalationStep) []EscalationStep {
	var out []EscalationStep
	for _, s := range steps {
		out = append(out, EscalationStep{
			Level:             s.Level,
			AfterUnackNs:      s.GetAfterUnacknowledged().AsDuration().Nanoseconds(),
			AfterUnresolvedNs: s.GetAfterUnresolved().AsDuration().Nanoseconds(),
			Notify:            s.Notify,
		})
	}
	return out
}

func validateSteps(steps []EscalationStep) error {
	seen := map[int32]bool{}
	for _, s := range steps {
		if s.Level <= 0 {
			return fmt.Errorf("升级级别必须为正，得到 %d", s.Level)
		}
		if seen[s.Level] {
			return fmt.Errorf("升级级别 %d 重复", s.Level)
		}
		seen[s.Level] = true
		if s.AfterUnackNs <= 0 && s.AfterUnresolvedNs <= 0 {
			return fmt.Errorf("级别 %d 未配置任何触发时长", s.Level)
		}
	}
	return nil
}

// ConfigToProto 将内部配置转回 proto（用于查询当前配置）。
func ConfigToProto(tv *ThresholdVersion) *pb.ThresholdVersionConfig {
	cfg := &pb.ThresholdVersionConfig{Version: tv.Version}
	for _, r := range tv.Rules {
		cfg.Rules = append(cfg.Rules, &pb.ThresholdRule{
			Name:               r.Name,
			MetricSensor:       r.MetricSensor,
			Unit:               r.Unit,
			Comparison:         r.Comparison,
			WarningLimit:       r.WarningLimit,
			CriticalLimit:      r.CriticalLimit,
			Corroborators:      append([]string(nil), r.Corroborators...),
			SummaryWarning:     r.SummaryWarning,
			SummaryCritical:    r.SummaryCritical,
			CrewActionWarning:  r.CrewActionWarning,
			CrewActionCritical: r.CrewActionCritical,
		})
	}
	return cfg
}

func fmtNs(ns int64) string {
	switch {
	case ns >= 60_000_000_000:
		return fmt.Sprintf("%.1fmin", float64(ns)/60_000_000_000)
	case ns >= 1_000_000_000:
		return fmt.Sprintf("%.1fs", float64(ns)/1_000_000_000)
	default:
		return fmt.Sprintf("%dms", ns/1_000_000)
	}
}

func fmtTs(ns int64) string {
	return strconv.FormatInt(ns, 10)
}
