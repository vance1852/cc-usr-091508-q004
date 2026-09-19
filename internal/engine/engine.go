// Package engine 实现生命保障告警的确定性判定核心。
//
// 引擎状态是事件日志的纯函数: 相同的事件序列 (无论一次跑完, 还是进程
// 中断后重放日志再续传) 必然得到相同的未决告警、升级历史与确认人。
//
// 时间模型:
//   - 持续窗口、阈值版本与任务阶段的选取都基于设备采样时钟 (device_time_ns);
//   - 升级计时、失联判定基于地面接收/日志时刻 (arrival_ns / ground_recv_ns);
//   - 设备时钟允许漂移, 两者绝不混用。
package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	lsv1 "lifesupport/gen/lifesupport/v1"
	"lifesupport/internal/eventlog"
)

// 事件类型 (写入日志的 type 列)。
const (
	EvSampleBatch = "sample_batch"
	EvThresholds  = "thresholds_activated"
	EvPhase       = "phase_changed"
	EvAck         = "ack"
	EvAssign      = "assign"
	EvEvidence    = "evidence"
	EvResolve     = "resolve"
)

// EscalationPolicy 描述某严重度在未确认状态下的升级节奏。
// 告警形成时级别为 1, 此后每经过 IntervalNs 未确认即升一级, 封顶 MaxLevel。
// Chain[level-1] 给出该级别通知的值班角色。
type EscalationPolicy struct {
	IntervalNs int64
	MaxLevel   int
	Chain      []string
}

// Config 是引擎的静态策略配置 (不随事件变化, 因此不参与重放)。
type Config struct {
	// Groups 关联传感器组: 组内任一必需传感器失联 => 证据不足。
	Groups map[string][]string
	// SilenceTimeoutNs 地面多久收不到某传感器的帧即判其失联。
	SilenceTimeoutNs int64
	// Escalation 按严重度的升级策略。
	Escalation map[lsv1.Severity]EscalationPolicy
	// Now 返回当前地面时刻 (ns), 仅用于查询时的派生计算, 不影响事件判定。
	Now func() int64
}

// ApplyResult 汇总一次事件应用的结果, 供 API 层返回。
type ApplyResult struct {
	EventUID         string
	DuplicateRequest bool     // 幂等键命中, 事件未重复应用
	Accepted         int32    // 接受的采样数 (仅遥测批次)
	Duplicates       int32    // 被去重的重复帧数 (仅遥测批次)
	NewAlertIDs      []string // 本次事件新形成的告警
}

type sensorState struct {
	samples     []*lsv1.Sample // 按 (device_time_ns, seq) 排序, 按 seq 去重
	seenSeq     map[int64]struct{}
	lastHeardNs int64 // 最近一次地面收到该传感器帧的时刻
	offsetEstNs int64 // 地面-设备钟差 EWMA
	offsetInit  bool
	duplicates  int64
}

type thresholdVersion struct {
	version       string
	effectiveFrom int64 // 设备时钟域
	rules         []*lsv1.RuleSpec
}

type phaseMark struct {
	effectiveFrom int64 // 设备时钟域
	phase         lsv1.MissionPhase
}

// Alert 是一条告警的完整生命周期状态。
type Alert struct {
	ID          string
	RuleID      string
	SensorID    string
	GroupID     string
	Severity    lsv1.Severity
	Status      lsv1.AlertStatus
	Summary     string
	CrewProc    string
	BreachStart int64 // 越限区间起点 (设备时钟)
	BreachEnd   int64 // 最近一次越限采样 (设备时钟)
	Formed      int64 // 持续窗口满足时刻 (设备时钟)
	Raised      int64 // 告警形成时的日志 (地面) 时刻
	Version     string
	Phase       lsv1.MissionPhase

	samples   []*lsv1.SampleRef // 形成告警的采样 (按设备时钟)
	sampleSeq map[int64]struct{}
	peak      float64
	last      float64

	Owner      string
	OwnerRole  lsv1.Role
	OwnerSince int64
	AckedBy    string
	AckedAt    int64
	Evidence   []*lsv1.EvidenceEntry
	ResolvedAt int64
	ResolvedBy string
}

// Engine 持有派生状态; 所有修改都经由 Apply 进入。
type Engine struct {
	mu  sync.Mutex
	cfg Config
	log *eventlog.Store

	sensors   map[string]*sensorState
	versions  []thresholdVersion // 按 effectiveFrom 升序
	phases    []phaseMark        // 按 effectiveFrom 升序
	alerts    map[string]*Alert
	alertIDs  []string // 创建顺序
	alertSeq  int
	shipNow   int64 // 已见最大设备采样时刻
	lastEvent int64 // 已应用的最大日志 ID
}

// Open 打开日志库并重放全部事件, 重建引擎状态。
func Open(dbPath string, cfg Config) (*Engine, error) {
	st, err := eventlog.Open(dbPath)
	if err != nil {
		return nil, err
	}
	e := &Engine{
		cfg:     cfg,
		log:     st,
		sensors: map[string]*sensorState{},
		alerts:  map[string]*Alert{},
	}
	if err := e.replay(); err != nil {
		st.Close()
		return nil, err
	}
	return e, nil
}

// OpenOn 在已有 Store 上构建引擎 (测试用)。
func OpenOn(st *eventlog.Store, cfg Config) (*Engine, error) {
	e := &Engine{
		cfg:     cfg,
		log:     st,
		sensors: map[string]*sensorState{},
		alerts:  map[string]*Alert{},
	}
	if err := e.replay(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *Engine) Close() error { return e.log.Close() }

func (e *Engine) replay() error {
	events, err := e.log.List()
	if err != nil {
		return err
	}
	for _, ev := range events {
		if err := e.mutate(ev); err != nil {
			return fmt.Errorf("replay event %d (%s): %w", ev.ID, ev.Type, err)
		}
	}
	return nil
}

// Apply 校验并追加一条事件, 然后应用到派生状态。
// 校验失败的事件不会落盘; 落盘成功的事件在重放时必然可应用。
func (e *Engine) Apply(evType string, payload proto.Message, uid string, arrivalNs int64) (*ApplyResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	data, err := proto.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if uid == "" {
		sum := sha256.Sum256(append([]byte(evType), data...))
		uid = evType + ":" + hex.EncodeToString(sum[:16])
	}
	if arrivalNs == 0 && e.cfg.Now != nil {
		arrivalNs = e.cfg.Now()
	}

	// 幂等键命中直接返回成功: 重试请求不应因状态已前进而报错。
	if dup, err := e.log.Has(uid); err != nil {
		return nil, err
	} else if dup {
		return &ApplyResult{EventUID: uid, DuplicateRequest: true}, nil
	}

	ev := eventlog.Event{UID: uid, Type: evType, Payload: data, ArrivalNs: arrivalNs}
	if err := e.validate(ev); err != nil {
		return nil, err
	}
	id, err := e.log.Append(ev)
	if errors.Is(err, eventlog.ErrDuplicate) {
		return &ApplyResult{EventUID: uid, DuplicateRequest: true}, nil
	}
	if err != nil {
		return nil, err
	}
	ev.ID = id
	res := &ApplyResult{EventUID: uid}
	if err := e.apply(ev, res); err != nil {
		// validate 已通过, 此处不应失败; 失败意味着 validate/mutate 不一致。
		return nil, fmt.Errorf("apply event %d: %w", id, err)
	}
	return res, nil
}

// mutate 重放路径: 事件已在日志中, 直接应用。
func (e *Engine) mutate(ev eventlog.Event) error {
	return e.apply(ev, &ApplyResult{})
}

func (e *Engine) apply(ev eventlog.Event, res *ApplyResult) error {
	res.EventUID = ev.UID
	switch ev.Type {
	case EvSampleBatch:
		var b lsv1.SampleBatchEvent
		if err := proto.Unmarshal(ev.Payload, &b); err != nil {
			return err
		}
		e.applySampleBatch(&b, ev.ArrivalNs, res)
	case EvThresholds:
		var t lsv1.ThresholdsActivatedEvent
		if err := proto.Unmarshal(ev.Payload, &t); err != nil {
			return err
		}
		e.applyThresholds(&t, ev.ArrivalNs)
	case EvPhase:
		var p lsv1.PhaseChangedEvent
		if err := proto.Unmarshal(ev.Payload, &p); err != nil {
			return err
		}
		e.applyPhase(&p, ev.ArrivalNs)
	case EvAck:
		var a lsv1.AckEvent
		if err := proto.Unmarshal(ev.Payload, &a); err != nil {
			return err
		}
		e.applyAck(&a, ev.ArrivalNs)
	case EvAssign:
		var a lsv1.AssignEvent
		if err := proto.Unmarshal(ev.Payload, &a); err != nil {
			return err
		}
		e.applyAssign(&a, ev.ArrivalNs)
	case EvEvidence:
		var v lsv1.EvidenceEvent
		if err := proto.Unmarshal(ev.Payload, &v); err != nil {
			return err
		}
		e.applyEvidence(&v, ev.ArrivalNs)
	case EvResolve:
		var r lsv1.ResolveEvent
		if err := proto.Unmarshal(ev.Payload, &r); err != nil {
			return err
		}
		e.applyResolve(&r, ev.ArrivalNs)
	default:
		return fmt.Errorf("unknown event type %q", ev.Type)
	}
	if ev.ID > e.lastEvent {
		e.lastEvent = ev.ID
	}
	return nil
}

// ---------------------------------------------------------------------------
// 校验: 在事件落盘前执行, 保证日志中只存在可重放的事件。
// ---------------------------------------------------------------------------

func (e *Engine) validate(ev eventlog.Event) error {
	switch ev.Type {
	case EvSampleBatch:
		var b lsv1.SampleBatchEvent
		if err := proto.Unmarshal(ev.Payload, &b); err != nil {
			return err
		}
		if len(b.Samples) == 0 {
			return errors.New("empty sample batch")
		}
		for i, s := range b.Samples {
			if s.SensorId == "" {
				return fmt.Errorf("sample %d: empty sensor_id", i)
			}
			if s.DeviceTimeNs <= 0 || s.GroundRecvNs <= 0 {
				return fmt.Errorf("sample %d: timestamps must be positive", i)
			}
		}
		return nil

	case EvThresholds:
		var t lsv1.ThresholdsActivatedEvent
		if err := proto.Unmarshal(ev.Payload, &t); err != nil {
			return err
		}
		if t.Version == "" {
			return errors.New("threshold version required")
		}
		if len(t.Rules) == 0 {
			return errors.New("threshold version must carry at least one rule")
		}
		for _, v := range e.versions {
			if v.version == t.Version {
				return fmt.Errorf("threshold version %q already activated", t.Version)
			}
		}
		// 版本时间线单调, 禁止追溯性生效, 保证旧结论不可被改写。
		if n := len(e.versions); n > 0 && t.EffectiveFromDeviceNs <= e.versions[n-1].effectiveFrom {
			return fmt.Errorf("effective_from %d not after previous version's %d",
				t.EffectiveFromDeviceNs, e.versions[n-1].effectiveFrom)
		}
		seen := map[string]bool{}
		for _, r := range t.Rules {
			if r.RuleId == "" || r.SensorId == "" {
				return errors.New("rule_id and sensor_id required")
			}
			if seen[r.RuleId] {
				return fmt.Errorf("duplicate rule_id %q", r.RuleId)
			}
			seen[r.RuleId] = true
			if r.WindowNs <= 0 || r.MaxGapNs <= 0 {
				return fmt.Errorf("rule %q: window and max_gap must be positive", r.RuleId)
			}
			if r.Direction == lsv1.RuleDirection_RULE_DIRECTION_UNSPECIFIED {
				return fmt.Errorf("rule %q: direction required", r.RuleId)
			}
			if r.Severity == lsv1.Severity_SEVERITY_UNSPECIFIED {
				return fmt.Errorf("rule %q: severity required", r.RuleId)
			}
		}
		return nil

	case EvPhase:
		var p lsv1.PhaseChangedEvent
		if err := proto.Unmarshal(ev.Payload, &p); err != nil {
			return err
		}
		if p.Phase == lsv1.MissionPhase_MISSION_PHASE_UNSPECIFIED {
			return errors.New("phase required")
		}
		if n := len(e.phases); n > 0 && p.EffectiveFromDeviceNs <= e.phases[n-1].effectiveFrom {
			return fmt.Errorf("phase effective_from %d not after previous %d",
				p.EffectiveFromDeviceNs, e.phases[n-1].effectiveFrom)
		}
		return nil

	case EvAck:
		var a lsv1.AckEvent
		if err := proto.Unmarshal(ev.Payload, &a); err != nil {
			return err
		}
		al := e.alerts[a.AlertId]
		if al == nil {
			return fmt.Errorf("alert %q not found", a.AlertId)
		}
		if a.Operator == "" {
			return errors.New("operator required")
		}
		if al.Status != lsv1.AlertStatus_ALERT_STATUS_OPEN {
			return fmt.Errorf("alert %q is %v, only OPEN can be acked", a.AlertId, al.Status)
		}
		return nil

	case EvAssign:
		var a lsv1.AssignEvent
		if err := proto.Unmarshal(ev.Payload, &a); err != nil {
			return err
		}
		al := e.alerts[a.AlertId]
		if al == nil {
			return fmt.Errorf("alert %q not found", a.AlertId)
		}
		if a.Operator == "" {
			return errors.New("assignee required")
		}
		if al.Status == lsv1.AlertStatus_ALERT_STATUS_RESOLVED {
			return fmt.Errorf("alert %q already resolved", a.AlertId)
		}
		return nil

	case EvEvidence:
		var v lsv1.EvidenceEvent
		if err := proto.Unmarshal(ev.Payload, &v); err != nil {
			return err
		}
		if e.alerts[v.AlertId] == nil {
			return fmt.Errorf("alert %q not found", v.AlertId)
		}
		if v.Operator == "" || v.Summary == "" {
			return errors.New("operator and summary required")
		}
		return nil

	case EvResolve:
		var r lsv1.ResolveEvent
		if err := proto.Unmarshal(ev.Payload, &r); err != nil {
			return err
		}
		al := e.alerts[r.AlertId]
		if al == nil {
			return fmt.Errorf("alert %q not found", r.AlertId)
		}
		if r.Operator == "" || r.Summary == "" {
			return errors.New("operator and resolution evidence summary required")
		}
		if al.Status == lsv1.AlertStatus_ALERT_STATUS_RESOLVED {
			return fmt.Errorf("alert %q already resolved", r.AlertId)
		}
		// 红色告警必须先确认, 才能追加解除证据并解除。
		if al.Severity == lsv1.Severity_SEVERITY_RED && al.Status != lsv1.AlertStatus_ALERT_STATUS_ACKED {
			return fmt.Errorf("red alert %q must be acked before resolve", r.AlertId)
		}
		return nil

	default:
		return fmt.Errorf("unknown event type %q", ev.Type)
	}
}

// ---------------------------------------------------------------------------
// 状态查询辅助
// ---------------------------------------------------------------------------

// versionAt 返回设备时刻 t 适用的阈值版本, 无则 nil。
func (e *Engine) versionAt(t int64) *thresholdVersion {
	var out *thresholdVersion
	for i := range e.versions {
		if e.versions[i].effectiveFrom <= t {
			out = &e.versions[i]
		} else {
			break
		}
	}
	return out
}

// phaseAt 返回设备时刻 t 的任务阶段, 未知则 UNSPECIFIED。
func (e *Engine) phaseAt(t int64) lsv1.MissionPhase {
	out := lsv1.MissionPhase_MISSION_PHASE_UNSPECIFIED
	for _, p := range e.phases {
		if p.effectiveFrom <= t {
			out = p.phase
		} else {
			break
		}
	}
	return out
}

// ShipNow 返回由遥测推得的星上当前时刻。
func (e *Engine) ShipNow() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.shipNow
}

// EventCount 返回事件日志中的事件数。
func (e *Engine) EventCount() (int, error) { return e.log.Len() }

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
