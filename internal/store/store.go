// Package store 以 SQLite 实现事件溯源存储：
//
//   - events 表是唯一事实来源（追加式事件日志）；
//   - samples/alerts/evaluations/escalations 等均为派生状态，
//     由事件按日志顺序确定性地应用得到，可随时重建；
//   - 每个事件在一个事务内完成“记录日志 + 更新派生状态”，
//     进程中断只会回滚未完成事务，重投同一事件流可稳定重建；
//   - 已确认的告警与证据物理上不可删除（无删除路径）。
package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	pb "lifesupport/gen/lifesupport/v1"
	"lifesupport/internal/core"
)

// 事件类型。
const (
	EvSamples   = "samples"
	EvThreshold = "threshold_version"
	EvPhase     = "mission_phase"
	EvAck       = "acknowledge"
	EvResolve   = "resolution"
	EvHeartbeat = "heartbeat"
)

// IngestResult 是一批采样的注入结果。
type IngestResult struct {
	Accepted   int
	Duplicates int
	Conflicts  int
	Replayed   bool // submission_id 已处理过，直接返回首次结果
}

// Alert 是派生的告警状态。
type Alert struct {
	ID               string
	Rule             string
	Severity         pb.Severity
	State            pb.AlertState
	OpenedDeviceNs   int64
	OpenedGroundNs   int64
	ThresholdVersion string
	Phase            pb.MissionPhase
	Insufficient     bool
	Missing          []string
	AckBy            string
	AckRole          pb.Role
	AckNote          string
	AckGroundNs      int64
	EscLevel         int32
	NextEscGroundNs  int64 // 0 = 无待升级
	ResolvedGroundNs int64
	MaxValue         float64
}

// Escalation 是一条升级记录。
type Escalation struct {
	Level       int32
	DueGroundNs int64
	RecordedNs  int64
	Reason      string
}

// Resolution 是一条解除证据。
type Resolution struct {
	Summary  string
	By       string
	Role     pb.Role
	GroundNs int64
	Auto     bool
}

// Acknowledgement 是一条确认（接管）记录。
type Acknowledgement struct {
	By       string
	Role     pb.Role
	Note     string
	GroundNs int64
}

// Evaluation 是一条判定记录（不可变审计）。
type Evaluation struct {
	ID               int64
	Rule             string
	EndDeviceNs      int64
	Verdict          pb.Verdict
	Severity         pb.Severity
	ThresholdVersion string
	Phase            pb.MissionPhase
	Detail           string
	GroundNs         int64
}

// SensorState 是传感器健康状态。
type SensorState struct {
	SensorID      string
	LastSeq       int64
	LastDeviceNs  int64
	LastGroundNs  int64
	DriftCount    int64
	ConflictCount int64
}

// Store 包装 SQLite。写路径串行化（单写者），读路径直接查询。
type Store struct {
	db *sql.DB
	mu sync.Mutex
}

// Open 打开（必要时创建）存储。path 可为文件路径或 ":memory:"。
func Open(path string) (*Store, error) {
	dsn := path
	if path == ":memory:" {
		dsn = "file:memdb?mode=memory&cache=shared"
	}
	db, err := sql.Open("sqlite3", dsn+"?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // 单写者，保证事件应用顺序
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close 关闭存储。
func (s *Store) Close() error { return s.db.Close() }

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS events(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL,
  payload TEXT NOT NULL,
  result TEXT NOT NULL DEFAULT '',
  ground_ns INTEGER NOT NULL,
  submission_id TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS events_submission ON events(submission_id) WHERE submission_id != '';

CREATE TABLE IF NOT EXISTS samples(
  sensor_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  frame_id TEXT NOT NULL,
  device_ns INTEGER NOT NULL,
  ground_ns INTEGER NOT NULL,
  value REAL NOT NULL,
  unit TEXT NOT NULL,
  quality INTEGER NOT NULL,
  event_seq INTEGER NOT NULL,
  PRIMARY KEY(sensor_id, seq)
);
CREATE UNIQUE INDEX IF NOT EXISTS samples_frame ON samples(sensor_id, frame_id) WHERE frame_id != '';
CREATE INDEX IF NOT EXISTS samples_device ON samples(sensor_id, device_ns);

CREATE TABLE IF NOT EXISTS threshold_versions(
  version TEXT PRIMARY KEY,
  config_json TEXT NOT NULL,
  effective_device_ns INTEGER NOT NULL,
  event_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS phases(
  event_seq INTEGER PRIMARY KEY,
  phase TEXT NOT NULL,
  effective_device_ns INTEGER NOT NULL,
  ground_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS evaluations(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  rule TEXT NOT NULL,
  end_device_ns INTEGER NOT NULL,
  verdict TEXT NOT NULL,
  severity INTEGER NOT NULL,
  threshold_version TEXT NOT NULL,
  phase TEXT NOT NULL,
  detail TEXT NOT NULL,
  event_seq INTEGER NOT NULL,
  ground_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts(
  id TEXT PRIMARY KEY,
  rule TEXT NOT NULL,
  severity INTEGER NOT NULL,
  state TEXT NOT NULL,
  opened_device_ns INTEGER NOT NULL,
  opened_ground_ns INTEGER NOT NULL,
  opened_event_seq INTEGER NOT NULL,
  threshold_version TEXT NOT NULL,
  phase TEXT NOT NULL,
  insufficient INTEGER NOT NULL DEFAULT 0,
  missing_json TEXT NOT NULL DEFAULT '[]',
  ack_by TEXT NOT NULL DEFAULT '',
  ack_role INTEGER NOT NULL DEFAULT 0,
  ack_note TEXT NOT NULL DEFAULT '',
  ack_ground_ns INTEGER NOT NULL DEFAULT 0,
  esc_level INTEGER NOT NULL DEFAULT 0,
  next_esc_ground_ns INTEGER NOT NULL DEFAULT 0,
  resolved_ground_ns INTEGER NOT NULL DEFAULT 0,
  max_value REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS alert_samples(
  alert_id TEXT NOT NULL,
  sensor_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  PRIMARY KEY(alert_id, sensor_id, seq)
);

CREATE TABLE IF NOT EXISTS escalations(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  level INTEGER NOT NULL,
  due_ground_ns INTEGER NOT NULL,
  recorded_ground_ns INTEGER NOT NULL,
  reason TEXT NOT NULL,
  event_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS resolutions(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  summary TEXT NOT NULL,
  by TEXT NOT NULL,
  role INTEGER NOT NULL,
  ground_ns INTEGER NOT NULL,
  auto INTEGER NOT NULL,
  event_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS acknowledgements(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  by TEXT NOT NULL,
  role INTEGER NOT NULL,
  note TEXT NOT NULL,
  ground_ns INTEGER NOT NULL,
  event_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sensor_state(
  sensor_id TEXT PRIMARY KEY,
  last_seq INTEGER NOT NULL,
  last_device_ns INTEGER NOT NULL,
  last_ground_ns INTEGER NOT NULL,
  drift_count INTEGER NOT NULL DEFAULT 0,
  conflict_count INTEGER NOT NULL DEFAULT 0
);
`)
	return err
}

// ---------------------------------------------------------------------------
// 事件载荷
// ---------------------------------------------------------------------------

type samplePayload struct {
	Samples []core.Sample `json:"samples"`
}

type phasePayload struct {
	Phase             pb.MissionPhase `json:"phase"`
	EffectiveDeviceNs int64           `json:"effective_device_ns"`
}

type ackPayload struct {
	AlertID string  `json:"alert_id"`
	By      string  `json:"by"`
	Role    pb.Role `json:"role"`
	Note    string  `json:"note"`
}

type resolutionPayload struct {
	AlertID string  `json:"alert_id"`
	By      string  `json:"by"`
	Role    pb.Role `json:"role"`
	Summary string  `json:"summary"`
	Auto    bool    `json:"auto"`
}

// event 是解码后的日志事件。
type event struct {
	seq        int64
	typ        string
	groundNs   int64
	samples    []core.Sample
	config     *core.ThresholdVersion
	phase      phasePayload
	ack        *ackPayload
	resolution *resolutionPayload
}

// ---------------------------------------------------------------------------
// 写路径
// ---------------------------------------------------------------------------

// IngestSamples 注入一批采样（同一事件、同一事务）。
// submissionID 非空时提供幂等：重复提交直接返回首次结果。
func (s *Store) IngestSamples(samples []core.Sample, submissionID string) (IngestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if submissionID != "" {
		var res IngestResult
		var resultJSON string
		err := s.db.QueryRow(`SELECT result FROM events WHERE submission_id = ?`, submissionID).Scan(&resultJSON)
		if err == nil {
			if json.Unmarshal([]byte(resultJSON), &res) == nil {
				res.Replayed = true
				return res, nil
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return IngestResult{}, err
		}
	}

	groundNs := int64(0)
	for _, sm := range samples {
		if sm.GroundNs > groundNs {
			groundNs = sm.GroundNs
		}
	}

	tx, err := s.db.Begin()
	if err != nil {
		return IngestResult{}, err
	}
	defer tx.Rollback()

	evSeq, err := insertEvent(tx, EvSamples, samplePayload{Samples: samples}, "", groundNs, submissionID)
	if err != nil {
		return IngestResult{}, err
	}
	res, err := s.applySamples(tx, evSeq, samples, groundNs)
	if err != nil {
		return IngestResult{}, err
	}
	if err := s.processEscalations(tx, evSeq, groundNs); err != nil {
		return IngestResult{}, err
	}
	resultJSON, _ := json.Marshal(res)
	if _, err := tx.Exec(`UPDATE events SET result = ? WHERE seq = ?`, resultJSON, evSeq); err != nil {
		return IngestResult{}, err
	}
	return res, tx.Commit()
}

// RegisterThresholdVersion 注册一套阈值版本（幂等：同版本同配置重复注册为无操作）。
func (s *Store) RegisterThresholdVersion(cfg *core.ThresholdVersion, groundNs int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	var existing string
	err = s.db.QueryRow(`SELECT config_json FROM threshold_versions WHERE version = ?`, cfg.Version).Scan(&existing)
	if err == nil {
		if existing == string(cfgJSON) {
			return nil // 幂等重放
		}
		return fmt.Errorf("阈值版本 %q 已存在且配置不同，版本不可变", cfg.Version)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	evSeq, err := insertEvent(tx, EvThreshold, json.RawMessage(cfgJSON), "", groundNs, "")
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO threshold_versions(version, config_json, effective_device_ns, event_seq) VALUES(?,?,?,?)`,
		cfg.Version, cfgJSON, cfg.EffectiveDeviceNs, evSeq); err != nil {
		return err
	}
	// 配置变化后用最新已知设备时刻重估全部规则（产生新的判定记录，不改写历史）。
	if err := s.reevaluateAll(tx, evSeq, groundNs); err != nil {
		return err
	}
	if err := s.processEscalations(tx, evSeq, groundNs); err != nil {
		return err
	}
	return tx.Commit()
}

// SetMissionPhase 记录任务阶段切换。
func (s *Store) SetMissionPhase(phase pb.MissionPhase, effectiveDeviceNs, groundNs int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	evSeq, err := insertEvent(tx, EvPhase, phasePayload{Phase: phase, EffectiveDeviceNs: effectiveDeviceNs}, "", groundNs, "")
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO phases(event_seq, phase, effective_device_ns, ground_ns) VALUES(?,?,?,?)`,
		evSeq, phase.String(), effectiveDeviceNs, groundNs); err != nil {
		return err
	}
	if err := s.reevaluateAll(tx, evSeq, groundNs); err != nil {
		return err
	}
	if err := s.processEscalations(tx, evSeq, groundNs); err != nil {
		return err
	}
	return tx.Commit()
}

// Acknowledge 确认告警（接管）。重复确认幂等；不同人确认视为移交并保留历史。
func (s *Store) Acknowledge(alertID, by string, role pb.Role, note string, groundNs int64) (*Alert, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	a, err := getAlertTx(tx, alertID)
	if err != nil {
		return nil, err
	}
	if a.State == pb.AlertState_ALERT_STATE_RESOLVED {
		return nil, fmt.Errorf("告警 %s 已解除，无需确认", alertID)
	}
	if a.State == pb.AlertState_ALERT_STATE_ACKNOWLEDGED && a.AckBy == by {
		return a, nil // 幂等
	}

	evSeq, err := insertEvent(tx, EvAck, ackPayload{AlertID: alertID, By: by, Role: role, Note: note}, "", groundNs, "")
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`INSERT INTO acknowledgements(alert_id, by, role, note, ground_ns, event_seq) VALUES(?,?,?,?,?,?)`,
		alertID, by, int32(role), note, groundNs, evSeq); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`UPDATE alerts SET state='ACKNOWLEDGED', ack_by=?, ack_role=?, ack_note=?, ack_ground_ns=? WHERE id=?`,
		by, int32(role), note, groundNs, alertID); err != nil {
		return nil, err
	}
	// 确认后升级时刻表切换为“已确认未解除”基准。
	if err := s.refreshAlertSchedule(tx, alertID); err != nil {
		return nil, err
	}
	if err := s.processEscalations(tx, evSeq, groundNs); err != nil {
		return nil, err
	}
	updated, err := getAlertTx(tx, alertID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return updated, nil
}

// AppendResolution 追加解除证据。告警从不删除；已解除的告警仍可继续追加证据。
func (s *Store) AppendResolution(alertID, by string, role pb.Role, summary string, groundNs int64) (*Alert, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := getAlertTx(tx, alertID); err != nil {
		return nil, err
	}
	evSeq, err := insertEvent(tx, EvResolve, resolutionPayload{AlertID: alertID, By: by, Role: role, Summary: summary}, "", groundNs, "")
	if err != nil {
		return nil, err
	}
	if err := applyResolutionTx(tx, evSeq, alertID, by, role, summary, false, groundNs); err != nil {
		return nil, err
	}
	// 与重放路径一致：每个事件都以升级处理收尾。
	if err := s.processEscalations(tx, evSeq, groundNs); err != nil {
		return nil, err
	}
	updated, err := getAlertTx(tx, alertID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return updated, nil
}

// Heartbeat 推进地面时钟（处理到期的升级）。用于后台定时与重放。
func (s *Store) Heartbeat(groundNs int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	evSeq, err := insertEvent(tx, EvHeartbeat, struct{}{}, "", groundNs, "")
	if err != nil {
		return err
	}
	if err := s.processEscalations(tx, evSeq, groundNs); err != nil {
		return err
	}
	return tx.Commit()
}

func applyResolutionTx(tx *sql.Tx, evSeq int64, alertID, by string, role pb.Role, summary string, auto bool, groundNs int64) error {
	if _, err := tx.Exec(`INSERT INTO resolutions(alert_id, summary, by, role, ground_ns, auto, event_seq) VALUES(?,?,?,?,?,?,?)`,
		alertID, summary, by, int32(role), groundNs, boolToInt(auto), evSeq); err != nil {
		return err
	}
	_, err := tx.Exec(`UPDATE alerts SET state='RESOLVED',
	  resolved_ground_ns = CASE WHEN resolved_ground_ns = 0 THEN ? ELSE resolved_ground_ns END,
	  next_esc_ground_ns = 0 WHERE id = ?`, groundNs, alertID)
	return err
}

// ---------------------------------------------------------------------------
// 采样应用（事件派生逻辑的核心）
// ---------------------------------------------------------------------------

func (s *Store) applySamples(tx *sql.Tx, evSeq int64, samples []core.Sample, eventGroundNs int64) (IngestResult, error) {
	var res IngestResult
	type trigger struct {
		sample  core.Sample
		rules   []string
		metrics map[string]string // 规则名 -> 主传感器
	}
	var triggers []trigger

	for _, sm := range samples {
		status, err := insertSampleTx(tx, evSeq, sm)
		if err != nil {
			return res, err
		}
		switch status {
		case sampleDuplicate:
			res.Duplicates++
		case sampleConflict:
			res.Conflicts++
		default:
			res.Accepted++
		}
		if status != sampleInserted {
			continue
		}
		// 漂移观测：设备时钟回退。
		var lastDev int64
		err = tx.QueryRow(`SELECT last_device_ns FROM sensor_state WHERE sensor_id = ?`, sm.SensorID).Scan(&lastDev)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return res, err
		}
		drift := int64(0)
		if err == nil && sm.DeviceNs < lastDev {
			drift = 1
		}
		if _, err := tx.Exec(`INSERT INTO sensor_state(sensor_id, last_seq, last_device_ns, last_ground_ns, drift_count)
		  VALUES(?,?,?,?,?)
		  ON CONFLICT(sensor_id) DO UPDATE SET
		    last_seq = MAX(last_seq, excluded.last_seq),
		    last_device_ns = MAX(last_device_ns, excluded.last_device_ns),
		    last_ground_ns = MAX(last_ground_ns, excluded.last_ground_ns),
		    drift_count = drift_count + excluded.drift_count`,
			sm.SensorID, sm.Seq, sm.DeviceNs, sm.GroundNs, drift); err != nil {
			return res, err
		}
		cfg, err := configAtTx(tx, sm.DeviceNs)
		if err != nil {
			return res, err
		}
		if cfg == nil {
			continue // 尚无生效阈值版本，无法判定
		}
		ruleNames := cfg.RulesForSensor(sm.SensorID)
		metrics := map[string]string{}
		for _, name := range ruleNames {
			if r, ok := cfg.RuleFor(name, pb.MissionPhase_MISSION_PHASE_UNSPECIFIED); ok {
				metrics[name] = r.MetricSensor
			}
		}
		triggers = append(triggers, trigger{sample: sm, rules: ruleNames, metrics: metrics})
	}

	// 逐采样逐规则判定（顺序固定，重放结果一致）。
	evaluated := map[string]bool{}
	affected := map[string]string{} // 规则名 -> 主传感器
	for _, tr := range triggers {
		for _, ruleName := range tr.rules {
			key := fmt.Sprintf("%s@%d", ruleName, tr.sample.DeviceNs)
			if evaluated[key] {
				continue
			}
			evaluated[key] = true
			affected[ruleName] = tr.metrics[ruleName]
			if err := s.evaluateRule(tx, evSeq, ruleName, tr.sample.DeviceNs, tr.sample.GroundNs); err != nil {
				return res, err
			}
		}
	}

	// 离线补传归并：补传的旧采样到达后，必须在主传感器最新设备时刻
	// 重新判定一次，使按原始序列归并后的完整证据链得到结论。
	var affectedNames []string
	for name := range affected {
		affectedNames = append(affectedNames, name)
	}
	sort.Strings(affectedNames)
	for _, ruleName := range affectedNames {
		metric := affected[ruleName]
		var frontier sql.NullInt64
		if err := tx.QueryRow(`SELECT MAX(device_ns) FROM samples WHERE sensor_id = ?`, metric).Scan(&frontier); err != nil {
			return res, err
		}
		if !frontier.Valid {
			continue
		}
		key := fmt.Sprintf("%s@%d", ruleName, frontier.Int64)
		if evaluated[key] {
			continue
		}
		if err := s.evaluateRule(tx, evSeq, ruleName, frontier.Int64, eventGroundNs); err != nil {
			return res, err
		}
	}
	return res, nil
}

type sampleInsertStatus int

const (
	sampleInserted sampleInsertStatus = iota
	sampleDuplicate
	sampleConflict
)

func insertSampleTx(tx *sql.Tx, evSeq int64, sm core.Sample) (sampleInsertStatus, error) {
	// 重复帧去重。
	if sm.FrameID != "" {
		var v float64
		var dev int64
		err := tx.QueryRow(`SELECT value, device_ns FROM samples WHERE sensor_id = ? AND frame_id = ?`, sm.SensorID, sm.FrameID).Scan(&v, &dev)
		if err == nil {
			if v == sm.Value && dev == sm.DeviceNs {
				return sampleDuplicate, nil
			}
			return sampleConflict, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return sampleInserted, err
		}
	}
	// 原始序列去重 / 冲突检测。
	var v float64
	var dev int64
	err := tx.QueryRow(`SELECT value, device_ns FROM samples WHERE sensor_id = ? AND seq = ?`, sm.SensorID, sm.Seq).Scan(&v, &dev)
	if err == nil {
		if v == sm.Value && dev == sm.DeviceNs {
			return sampleDuplicate, nil
		}
		if _, err := tx.Exec(`UPDATE sensor_state SET conflict_count = conflict_count + 1 WHERE sensor_id = ?`, sm.SensorID); err != nil {
			return sampleInserted, err
		}
		return sampleConflict, nil // 保留先到的，绝不覆盖
	} else if !errors.Is(err, sql.ErrNoRows) {
		return sampleInserted, err
	}
	_, err = tx.Exec(`INSERT INTO samples(sensor_id, seq, frame_id, device_ns, ground_ns, value, unit, quality, event_seq)
	  VALUES(?,?,?,?,?,?,?,?,?)`,
		sm.SensorID, sm.Seq, sm.FrameID, sm.DeviceNs, sm.GroundNs, sm.Value, sm.Unit, sm.Quality, evSeq)
	if err != nil {
		return sampleInserted, err
	}
	return sampleInserted, nil
}

// evaluateRule 在设备时刻 endNs 对一条规则判定并应用结论。
func (s *Store) evaluateRule(tx *sql.Tx, evSeq int64, ruleName string, endNs, groundNs int64) error {
	cfg, err := configAtTx(tx, endNs)
	if err != nil || cfg == nil {
		return err
	}
	phase, err := phaseAtTx(tx, endNs)
	if err != nil {
		return err
	}
	rule, ok := cfg.RuleFor(ruleName, phase)
	if !ok {
		return nil
	}

	lookback := max64(max64(rule.WarningWindowNs, rule.CriticalWindowNs), rule.RecoveryWindowNs) + 2*rule.MaxSampleGapNs + 1
	rows, err := tx.Query(`SELECT seq, frame_id, device_ns, ground_ns, value, unit, quality FROM samples
	  WHERE sensor_id = ? AND device_ns <= ? AND device_ns >= ? ORDER BY seq ASC`,
		rule.MetricSensor, endNs, endNs-lookback)
	if err != nil {
		return err
	}
	var samples []core.Sample
	for rows.Next() {
		var sm core.Sample
		if err := rows.Scan(&sm.Seq, &sm.FrameID, &sm.DeviceNs, &sm.GroundNs, &sm.Value, &sm.Unit, &sm.Quality); err != nil {
			rows.Close()
			return err
		}
		sm.SensorID = rule.MetricSensor
		samples = append(samples, sm)
	}
	if err := rows.Close(); err != nil {
		return err
	}

	corLast := map[string]int64{}
	for _, c := range rule.Corroborators {
		var last sql.NullInt64
		if err := tx.QueryRow(`SELECT MAX(device_ns) FROM samples WHERE sensor_id = ? AND device_ns <= ?`, c, endNs).Scan(&last); err != nil {
			return err
		}
		if last.Valid {
			corLast[c] = last.Int64
		}
	}

	res := core.Evaluate(rule, endNs, samples, corLast)

	if _, err := tx.Exec(`INSERT INTO evaluations(rule, end_device_ns, verdict, severity, threshold_version, phase, detail, event_seq, ground_ns)
	  VALUES(?,?,?,?,?,?,?,?,?)`,
		ruleName, endNs, verdictName(res.Verdict), int32(res.Severity), cfg.Version, phase.String(), res.Detail, evSeq, groundNs); err != nil {
		return err
	}

	switch res.Verdict {
	case pb.Verdict_VERDICT_SUSTAINED_BREACH:
		return s.applyBreach(tx, evSeq, cfg, rule, phase, res, groundNs)
	case pb.Verdict_VERDICT_NOMINAL:
		return s.applyRecovery(tx, evSeq, rule, res, groundNs)
	case pb.Verdict_VERDICT_INSUFFICIENT_EVIDENCE:
		// 未决告警标记证据不足；绝不因此解除或推断安全。
		missingJSON, _ := json.Marshal(res.Missing)
		_, err := tx.Exec(`UPDATE alerts SET insufficient = 1, missing_json = ? WHERE rule = ? AND state != 'RESOLVED'`,
			missingJSON, ruleName)
		return err
	}
	return nil
}

func (s *Store) applyBreach(tx *sql.Tx, evSeq int64, cfg *core.ThresholdVersion, rule core.ThresholdRule,
	phase pb.MissionPhase, res core.EvalResult, groundNs int64) error {

	// 同规则未决告警：延续（同一事件）；否则新开。
	var a Alert
	err := tx.QueryRow(`SELECT id, severity, opened_device_ns, max_value FROM alerts WHERE rule = ? AND state != 'RESOLVED' ORDER BY opened_ground_ns ASC LIMIT 1`,
		rule.Name).Scan(&a.ID, &a.Severity, &a.OpenedDeviceNs, &a.MaxValue)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// 新开告警。若同一越限段曾被人工解除而遥测仍在越限，
		// 以确定性后缀生成新实例：旧告警与解除证据原样保留。
		a.ID = core.AlertID(rule.Name, res.RunStartNs)
		for n := 2; ; n++ {
			var cnt int
			if err := tx.QueryRow(`SELECT COUNT(1) FROM alerts WHERE id = ?`, a.ID).Scan(&cnt); err != nil {
				return err
			}
			if cnt == 0 {
				break
			}
			a.ID = fmt.Sprintf("%s-r%d", core.AlertID(rule.Name, res.RunStartNs), n)
		}
		a.OpenedDeviceNs = res.RunStartNs
		if _, err := tx.Exec(`INSERT INTO alerts(id, rule, severity, state, opened_device_ns, opened_ground_ns, opened_event_seq,
		    threshold_version, phase, max_value)
		  VALUES(?,?,?,?,?,?,?,?,?,?)`,
			a.ID, rule.Name, int32(res.Severity), "OPEN", res.RunStartNs, groundNs, evSeq,
			cfg.Version, phase.String(), maxFloat(res.Evidence)); err != nil {
			return err
		}
		a.Severity = res.Severity
		a.MaxValue = maxFloat(res.Evidence)
	case err != nil:
		return err
	default:
		newSev := res.Severity
		if sevRank(a.Severity) > sevRank(newSev) {
			newSev = a.Severity
		}
		openedDev := a.OpenedDeviceNs
		if res.RunStartNs < openedDev {
			openedDev = res.RunStartNs
		}
		if _, err := tx.Exec(`UPDATE alerts SET severity = ?, insufficient = 0, missing_json = '[]',
		  opened_device_ns = ?, max_value = ? WHERE id = ?`,
			int32(newSev), openedDev, maxFloat2(a.MaxValue, res.Evidence), a.ID); err != nil {
			return err
		}
	}

	for _, sm := range res.Evidence {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO alert_samples(alert_id, sensor_id, seq) VALUES(?,?,?)`,
			a.ID, sm.SensorID, sm.Seq); err != nil {
			return err
		}
	}
	return s.refreshAlertSchedule(tx, a.ID)
}

func (s *Store) applyRecovery(tx *sql.Tx, evSeq int64, rule core.ThresholdRule, res core.EvalResult, groundNs int64) error {
	if res.NominalSinceNs == 0 || res.EndDeviceNs-res.NominalSinceNs < rule.RecoveryWindowNs {
		return nil
	}
	var id string
	var openedDev int64
	err := tx.QueryRow(`SELECT id, opened_device_ns FROM alerts WHERE rule = ? AND state != 'RESOLVED' ORDER BY opened_ground_ns ASC LIMIT 1`,
		rule.Name).Scan(&id, &openedDev)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if res.NominalSinceNs <= openedDev {
		return nil // 正常段在告警开启之前（补传旧数据），不构成解除
	}
	summary := fmt.Sprintf("遥测证实恢复正常：%s 自设备时刻 %d 起持续正常满恢复窗口", rule.MetricSensor, res.NominalSinceNs)
	return applyResolutionTx(tx, evSeq, id, "system", pb.Role_ROLE_FLIGHT_CONTROL, summary, true, groundNs)
}

// ---------------------------------------------------------------------------
// 升级处理
// ---------------------------------------------------------------------------

// processEscalations 以地面时刻 groundNs 处理全部未解除告警的到期升级。
func (s *Store) processEscalations(tx *sql.Tx, evSeq, groundNs int64) error {
	if groundNs <= 0 {
		return nil
	}
	rows, err := tx.Query(`SELECT id, severity, state, opened_ground_ns, ack_ground_ns, esc_level, threshold_version
	  FROM alerts WHERE state != 'RESOLVED'`)
	if err != nil {
		return err
	}
	type row struct {
		id       string
		severity int32
		state    string
		openedG  int64
		ackG     int64
		escLevel int32
		tv       string
	}
	var list []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.severity, &r.state, &r.openedG, &r.ackG, &r.escLevel, &r.tv); err != nil {
			rows.Close()
			return err
		}
		list = append(list, r)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	sort.Slice(list, func(i, j int) bool { return list[i].id < list[j].id })

	for _, r := range list {
		cfg, err := configByVersionTx(tx, r.tv)
		if err != nil || cfg == nil {
			continue
		}
		state := pb.AlertState_ALERT_STATE_OPEN
		if r.state == "ACKNOWLEDGED" {
			state = pb.AlertState_ALERT_STATE_ACKNOWLEDGED
		}
		dues := core.EscalationSchedule(cfg.Policy, pb.Severity(r.severity), state, r.openedG, r.ackG, r.escLevel)
		maxLevel := r.escLevel
		for _, d := range dues {
			if d.DueNs <= groundNs {
				if _, err := tx.Exec(`INSERT INTO escalations(alert_id, level, due_ground_ns, recorded_ground_ns, reason, event_seq)
				  VALUES(?,?,?,?,?,?)`, r.id, d.Level, d.DueNs, groundNs, "通知: "+d.Notify, evSeq); err != nil {
					return err
				}
				if d.Level > maxLevel {
					maxLevel = d.Level
				}
			}
		}
		// 重算下一次升级时刻。
		var next int64
		for _, d := range core.EscalationSchedule(cfg.Policy, pb.Severity(r.severity), state, r.openedG, r.ackG, maxLevel) {
			if d.DueNs > groundNs {
				next = d.DueNs
				break
			}
		}
		if _, err := tx.Exec(`UPDATE alerts SET esc_level = ?, next_esc_ground_ns = ? WHERE id = ?`, maxLevel, next, r.id); err != nil {
			return err
		}
	}
	return nil
}

// refreshAlertSchedule 在状态变化后重算单条告警的下一升级时刻。
func (s *Store) refreshAlertSchedule(tx *sql.Tx, alertID string) error {
	var (
		sev      int32
		state    string
		openedG  int64
		ackG     int64
		escLevel int32
		tv       string
	)
	err := tx.QueryRow(`SELECT severity, state, opened_ground_ns, ack_ground_ns, esc_level, threshold_version FROM alerts WHERE id = ?`,
		alertID).Scan(&sev, &state, &openedG, &ackG, &escLevel, &tv)
	if err != nil {
		return err
	}
	if state == "RESOLVED" {
		_, err := tx.Exec(`UPDATE alerts SET next_esc_ground_ns = 0 WHERE id = ?`, alertID)
		return err
	}
	cfg, err := configByVersionTx(tx, tv)
	if err != nil || cfg == nil {
		return err
	}
	st := pb.AlertState_ALERT_STATE_OPEN
	if state == "ACKNOWLEDGED" {
		st = pb.AlertState_ALERT_STATE_ACKNOWLEDGED
	}
	var next int64
	for _, d := range core.EscalationSchedule(cfg.Policy, pb.Severity(sev), st, openedG, ackG, escLevel) {
		next = d.DueNs
		break
	}
	_, err = tx.Exec(`UPDATE alerts SET next_esc_ground_ns = ? WHERE id = ?`, next, alertID)
	return err
}

// reevaluateAll 在配置/阶段变化后，以各规则主传感器最新设备时刻重估。
func (s *Store) reevaluateAll(tx *sql.Tx, evSeq, groundNs int64) error {
	rows, err := tx.Query(`SELECT sensor_id, MAX(device_ns) FROM samples GROUP BY sensor_id ORDER BY sensor_id`)
	if err != nil {
		return err
	}
	type last struct {
		sensor string
		devNs  int64
	}
	var lasts []last
	for rows.Next() {
		var l last
		if err := rows.Scan(&l.sensor, &l.devNs); err != nil {
			rows.Close()
			return err
		}
		lasts = append(lasts, l)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	done := map[string]bool{}
	for _, l := range lasts {
		cfg, err := configAtTx(tx, l.devNs)
		if err != nil || cfg == nil {
			continue
		}
		for _, ruleName := range cfg.RulesForSensor(l.sensor) {
			key := fmt.Sprintf("%s@%d", ruleName, l.devNs)
			if done[key] {
				continue
			}
			done[key] = true
			if err := s.evaluateRule(tx, evSeq, ruleName, l.devNs, groundNs); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// 重建（进程中断/重放）
// ---------------------------------------------------------------------------

// Rebuild 清空派生状态并按事件日志顺序重放，得到与原先一致的状态。
func (s *Store) Rebuild() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows, err := s.db.Query(`SELECT seq, type, payload, ground_ns FROM events ORDER BY seq ASC`)
	if err != nil {
		return err
	}
	var raw []struct {
		seq      int64
		typ      string
		payload  string
		groundNs int64
	}
	for rows.Next() {
		var r struct {
			seq      int64
			typ      string
			payload  string
			groundNs int64
		}
		if err := rows.Scan(&r.seq, &r.typ, &r.payload, &r.groundNs); err != nil {
			rows.Close()
			return err
		}
		raw = append(raw, r)
	}
	if err := rows.Close(); err != nil {
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, tbl := range []string{"samples", "threshold_versions", "phases", "evaluations", "alerts",
		"alert_samples", "escalations", "resolutions", "acknowledgements", "sensor_state"} {
		if _, err := tx.Exec(`DELETE FROM ` + tbl); err != nil {
			return err
		}
	}
	// 重置派生表的自增序列，保证重建后行号与首次应用一致。
	for _, tbl := range []string{"evaluations", "escalations", "resolutions", "acknowledgements"} {
		if _, err := tx.Exec(`DELETE FROM sqlite_sequence WHERE name = ?`, tbl); err != nil {
			return err
		}
	}
	for _, r := range raw {
		ev, err := decodeEvent(r.seq, r.typ, r.payload, r.groundNs)
		if err != nil {
			return err
		}
		if err := s.applyEvent(tx, ev); err != nil {
			return fmt.Errorf("重放事件 %d (%s): %w", r.seq, r.typ, err)
		}
	}
	return tx.Commit()
}

func decodeEvent(seq int64, typ, payload string, groundNs int64) (*event, error) {
	ev := &event{seq: seq, typ: typ, groundNs: groundNs}
	switch typ {
	case EvSamples:
		var p samplePayload
		if err := json.Unmarshal([]byte(payload), &p); err != nil {
			return nil, err
		}
		ev.samples = p.Samples
	case EvThreshold:
		var cfg core.ThresholdVersion
		if err := json.Unmarshal([]byte(payload), &cfg); err != nil {
			return nil, err
		}
		ev.config = &cfg
	case EvPhase:
		if err := json.Unmarshal([]byte(payload), &ev.phase); err != nil {
			return nil, err
		}
	case EvAck:
		var p ackPayload
		if err := json.Unmarshal([]byte(payload), &p); err != nil {
			return nil, err
		}
		ev.ack = &p
	case EvResolve:
		var p resolutionPayload
		if err := json.Unmarshal([]byte(payload), &p); err != nil {
			return nil, err
		}
		ev.resolution = &p
	case EvHeartbeat:
	default:
		return nil, fmt.Errorf("未知事件类型 %q", typ)
	}
	return ev, nil
}

// applyEvent 仅由 Rebuild 使用：重放单个事件的派生效果。
func (s *Store) applyEvent(tx *sql.Tx, ev *event) error {
	switch ev.typ {
	case EvSamples:
		if _, err := s.applySamples(tx, ev.seq, ev.samples, ev.groundNs); err != nil {
			return err
		}
	case EvThreshold:
		cfgJSON, _ := json.Marshal(ev.config)
		if _, err := tx.Exec(`INSERT INTO threshold_versions(version, config_json, effective_device_ns, event_seq) VALUES(?,?,?,?)`,
			ev.config.Version, cfgJSON, ev.config.EffectiveDeviceNs, ev.seq); err != nil {
			return err
		}
		if err := s.reevaluateAll(tx, ev.seq, ev.groundNs); err != nil {
			return err
		}
	case EvPhase:
		if _, err := tx.Exec(`INSERT INTO phases(event_seq, phase, effective_device_ns, ground_ns) VALUES(?,?,?,?)`,
			ev.seq, ev.phase.Phase.String(), ev.phase.EffectiveDeviceNs, ev.groundNs); err != nil {
			return err
		}
		if err := s.reevaluateAll(tx, ev.seq, ev.groundNs); err != nil {
			return err
		}
	case EvAck:
		if _, err := tx.Exec(`INSERT INTO acknowledgements(alert_id, by, role, note, ground_ns, event_seq) VALUES(?,?,?,?,?,?)`,
			ev.ack.AlertID, ev.ack.By, int32(ev.ack.Role), ev.ack.Note, ev.groundNs, ev.seq); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE alerts SET state='ACKNOWLEDGED', ack_by=?, ack_role=?, ack_note=?, ack_ground_ns=? WHERE id=?`,
			ev.ack.By, int32(ev.ack.Role), ev.ack.Note, ev.groundNs, ev.ack.AlertID); err != nil {
			return err
		}
		if err := s.refreshAlertSchedule(tx, ev.ack.AlertID); err != nil {
			return err
		}
	case EvResolve:
		if err := applyResolutionTx(tx, ev.seq, ev.resolution.AlertID, ev.resolution.By, ev.resolution.Role,
			ev.resolution.Summary, ev.resolution.Auto, ev.groundNs); err != nil {
			return err
		}
	case EvHeartbeat:
	}
	return s.processEscalations(tx, ev.seq, ev.groundNs)
}

// ---------------------------------------------------------------------------
// 查询
// ---------------------------------------------------------------------------

// GetAlert 读取单条告警。
func (s *Store) GetAlert(id string) (*Alert, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return getAlertTx(s.db, id)
}

type querier interface {
	QueryRow(query string, args ...any) *sql.Row
	Query(query string, args ...any) (*sql.Rows, error)
}

func getAlertTx(q querier, id string) (*Alert, error) {
	var a Alert
	var state, phase, missing string
	var sev, ackRole int32
	err := q.QueryRow(`SELECT id, rule, severity, state, opened_device_ns, opened_ground_ns, threshold_version,
	  phase, insufficient, missing_json, ack_by, ack_role, ack_note, ack_ground_ns, esc_level, next_esc_ground_ns,
	  resolved_ground_ns, max_value FROM alerts WHERE id = ?`, id).
		Scan(&a.ID, &a.Rule, &sev, &state, &a.OpenedDeviceNs, &a.OpenedGroundNs, &a.ThresholdVersion,
			&phase, &a.Insufficient, &missing, &a.AckBy, &ackRole, &a.AckNote, &a.AckGroundNs, &a.EscLevel,
			&a.NextEscGroundNs, &a.ResolvedGroundNs, &a.MaxValue)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("告警 %q 不存在", id)
	}
	if err != nil {
		return nil, err
	}
	a.Severity = pb.Severity(sev)
	a.State = alertStateFromName(state)
	a.Phase = missionPhaseFromName(phase)
	a.AckRole = pb.Role(ackRole)
	_ = json.Unmarshal([]byte(missing), &a.Missing)
	return &a, nil
}

// ListAlerts 返回全部告警：未决在前（按开启时刻升序），已解除在后。
func (s *Store) ListAlerts() ([]Alert, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT id FROM alerts ORDER BY CASE state WHEN 'RESOLVED' THEN 1 ELSE 0 END, opened_ground_ns ASC, id ASC`)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var out []Alert
	for _, id := range ids {
		a, err := getAlertTx(s.db, id)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, nil
}

// AlertEvidence 返回形成告警的采样（按设备时刻、传感器、序列排序）。
func (s *Store) AlertEvidence(alertID string) ([]core.Sample, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT sa.sensor_id, sa.seq, s.frame_id, s.device_ns, s.ground_ns, s.value, s.unit, s.quality
	  FROM alert_samples sa JOIN samples s ON s.sensor_id = sa.sensor_id AND s.seq = sa.seq
	  WHERE sa.alert_id = ? ORDER BY s.device_ns ASC, sa.sensor_id ASC, sa.seq ASC`, alertID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.Sample
	for rows.Next() {
		var sm core.Sample
		if err := rows.Scan(&sm.SensorID, &sm.Seq, &sm.FrameID, &sm.DeviceNs, &sm.GroundNs, &sm.Value, &sm.Unit, &sm.Quality); err != nil {
			return nil, err
		}
		out = append(out, sm)
	}
	return out, rows.Err()
}

// ListEscalations 返回告警的升级历史。
func (s *Store) ListEscalations(alertID string) ([]Escalation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT level, due_ground_ns, recorded_ground_ns, reason FROM escalations WHERE alert_id = ? ORDER BY id ASC`, alertID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Escalation
	for rows.Next() {
		var e Escalation
		if err := rows.Scan(&e.Level, &e.DueGroundNs, &e.RecordedNs, &e.Reason); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ListResolutions 返回解除证据链。
func (s *Store) ListResolutions(alertID string) ([]Resolution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT summary, by, role, ground_ns, auto FROM resolutions WHERE alert_id = ? ORDER BY id ASC`, alertID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Resolution
	for rows.Next() {
		var r Resolution
		var role int32
		var auto int
		if err := rows.Scan(&r.Summary, &r.By, &role, &r.GroundNs, &auto); err != nil {
			return nil, err
		}
		r.Role = pb.Role(role)
		r.Auto = auto != 0
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListAcknowledgements 返回确认（接管）历史。
func (s *Store) ListAcknowledgements(alertID string) ([]Acknowledgement, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT by, role, note, ground_ns FROM acknowledgements WHERE alert_id = ? ORDER BY id ASC`, alertID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Acknowledgement
	for rows.Next() {
		var a Acknowledgement
		var role int32
		if err := rows.Scan(&a.By, &role, &a.Note, &a.GroundNs); err != nil {
			return nil, err
		}
		a.Role = pb.Role(role)
		out = append(out, a)
	}
	return out, rows.Err()
}

// ListEvaluations 返回判定记录（审计），按记录号倒序。
func (s *Store) ListEvaluations(rule string, limit int) ([]Evaluation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	q := `SELECT id, rule, end_device_ns, verdict, severity, threshold_version, phase, detail, ground_ns FROM evaluations`
	args := []any{}
	if rule != "" {
		q += ` WHERE rule = ?`
		args = append(args, rule)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Evaluation
	for rows.Next() {
		var e Evaluation
		var verdict, phase string
		var sev int32
		if err := rows.Scan(&e.ID, &e.Rule, &e.EndDeviceNs, &verdict, &sev, &e.ThresholdVersion, &phase, &e.Detail, &e.GroundNs); err != nil {
			return nil, err
		}
		e.Verdict = verdictFromName(verdict)
		e.Severity = pb.Severity(sev)
		e.Phase = missionPhaseFromName(phase)
		out = append(out, e)
	}
	return out, rows.Err()
}

// SensorHealth 返回全部传感器健康状态。
func (s *Store) SensorHealth() ([]SensorState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT sensor_id, last_seq, last_device_ns, last_ground_ns, drift_count, conflict_count FROM sensor_state ORDER BY sensor_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SensorState
	for rows.Next() {
		var st SensorState
		if err := rows.Scan(&st.SensorID, &st.LastSeq, &st.LastDeviceNs, &st.LastGroundNs, &st.DriftCount, &st.ConflictCount); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// CurrentConfig 返回当前生效（按设备时刻最新）的阈值版本。
func (s *Store) CurrentConfig() (*core.ThresholdVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.db.QueryRow(`SELECT config_json FROM threshold_versions ORDER BY effective_device_ns DESC, event_seq DESC LIMIT 1`)
	var cfgJSON string
	if err := row.Scan(&cfgJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	var cfg core.ThresholdVersion
	if err := json.Unmarshal([]byte(cfgJSON), &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ConfigByVersion 按版本号读取配置。
func (s *Store) ConfigByVersion(version string) (*core.ThresholdVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return configByVersionTx(s.db, version)
}

func configByVersionTx(q querier, version string) (*core.ThresholdVersion, error) {
	var cfgJSON string
	err := q.QueryRow(`SELECT config_json FROM threshold_versions WHERE version = ?`, version).Scan(&cfgJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cfg core.ThresholdVersion
	if err := json.Unmarshal([]byte(cfgJSON), &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func configAtTx(q querier, deviceNs int64) (*core.ThresholdVersion, error) {
	var cfgJSON string
	err := q.QueryRow(`SELECT config_json FROM threshold_versions WHERE effective_device_ns <= ? ORDER BY effective_device_ns DESC, event_seq DESC LIMIT 1`, deviceNs).Scan(&cfgJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cfg core.ThresholdVersion
	if err := json.Unmarshal([]byte(cfgJSON), &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func phaseAtTx(q querier, deviceNs int64) (pb.MissionPhase, error) {
	var phase string
	err := q.QueryRow(`SELECT phase FROM phases WHERE effective_device_ns <= ? ORDER BY effective_device_ns DESC, event_seq DESC LIMIT 1`, deviceNs).Scan(&phase)
	if errors.Is(err, sql.ErrNoRows) {
		return pb.MissionPhase_MISSION_PHASE_UNSPECIFIED, nil
	}
	if err != nil {
		return pb.MissionPhase_MISSION_PHASE_UNSPECIFIED, err
	}
	return missionPhaseFromName(phase), nil
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

func insertEvent(tx *sql.Tx, typ string, payload any, result string, groundNs int64, submissionID string) (int64, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	res, err := tx.Exec(`INSERT INTO events(type, payload, result, ground_ns, submission_id) VALUES(?,?,?,?,?)`,
		typ, payloadJSON, result, groundNs, submissionID)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func verdictName(v pb.Verdict) string {
	switch v {
	case pb.Verdict_VERDICT_NOMINAL:
		return "NOMINAL"
	case pb.Verdict_VERDICT_TRANSIENT:
		return "TRANSIENT"
	case pb.Verdict_VERDICT_SUSTAINED_BREACH:
		return "SUSTAINED_BREACH"
	case pb.Verdict_VERDICT_INSUFFICIENT_EVIDENCE:
		return "INSUFFICIENT_EVIDENCE"
	}
	return "UNSPECIFIED"
}

func verdictFromName(name string) pb.Verdict {
	switch name {
	case "NOMINAL":
		return pb.Verdict_VERDICT_NOMINAL
	case "TRANSIENT":
		return pb.Verdict_VERDICT_TRANSIENT
	case "SUSTAINED_BREACH":
		return pb.Verdict_VERDICT_SUSTAINED_BREACH
	case "INSUFFICIENT_EVIDENCE":
		return pb.Verdict_VERDICT_INSUFFICIENT_EVIDENCE
	}
	return pb.Verdict_VERDICT_UNSPECIFIED
}

func alertStateFromName(name string) pb.AlertState {
	switch name {
	case "OPEN":
		return pb.AlertState_ALERT_STATE_OPEN
	case "ACKNOWLEDGED":
		return pb.AlertState_ALERT_STATE_ACKNOWLEDGED
	case "RESOLVED":
		return pb.AlertState_ALERT_STATE_RESOLVED
	}
	return pb.AlertState_ALERT_STATE_UNSPECIFIED
}

func missionPhaseFromName(name string) pb.MissionPhase {
	if v, ok := pb.MissionPhase_value[name]; ok {
		return pb.MissionPhase(v)
	}
	return pb.MissionPhase_MISSION_PHASE_UNSPECIFIED
}

func sevRank(s pb.Severity) int {
	if s == pb.Severity_SEVERITY_CRITICAL {
		return 2
	}
	if s == pb.Severity_SEVERITY_WARNING {
		return 1
	}
	return 0
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxFloat(samples []core.Sample) float64 {
	if len(samples) == 0 {
		return 0
	}
	m := samples[0].Value
	for _, sm := range samples[1:] {
		if sm.Value > m {
			m = sm.Value
		}
	}
	return m
}

func maxFloat2(cur float64, samples []core.Sample) float64 {
	m := maxFloat(samples)
	if cur > m {
		return cur
	}
	return m
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
