// Package eventlog 提供追加式 SQLite 事件日志。
//
// 系统中一切状态变化 (遥测批次、阈值版本启用、任务阶段切换、确认/接管/
// 证据/解除) 都以不可变事件的形式按到达顺序写入该日志。告警引擎的内存
// 状态是对日志顺序重放的纯函数结果, 因此进程中断后重开数据库即可稳定
// 重建未决告警、升级历史与确认人。
package eventlog

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// Event 是日志中的一条记录。ID 由数据库分配, 全局单调递增, 即重放顺序。
type Event struct {
	ID        int64
	UID       string // 幂等键, 唯一
	Type      string // 事件类型, 如 "sample_batch"
	Payload   []byte // 事件负载 (protobuf 序列化)
	ArrivalNs int64  // 地面 (日志) 接收时刻, 作为升级等时间推导的基准
}

// ErrDuplicate 表示幂等键已存在, 本次写入被安全忽略。
var ErrDuplicate = errors.New("eventlog: duplicate event uid")

type Store struct {
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    uid        TEXT NOT NULL UNIQUE,
    type       TEXT NOT NULL,
    payload    BLOB NOT NULL,
    arrival_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
`

// Open 打开 (必要时创建) 位于 path 的日志库。path 为 ":memory:" 时使用内存库。
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	// 单写者: 避免 "database is locked" 并保证顺序追加。
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Append 追加一条事件。UID 冲突时返回 (0, ErrDuplicate), 日志保持不变。
func (s *Store) Append(ev Event) (int64, error) {
	res, err := s.db.Exec(
		`INSERT INTO events (uid, type, payload, arrival_ns) VALUES (?, ?, ?, ?)
		 ON CONFLICT(uid) DO NOTHING`,
		ev.UID, ev.Type, ev.Payload, ev.ArrivalNs)
	if err != nil {
		return 0, fmt.Errorf("append event: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, ErrDuplicate
	}
	return res.LastInsertId()
}

// List 按日志顺序返回全部事件, 用于启动时重放。
func (s *Store) List() ([]Event, error) {
	rows, err := s.db.Query(`SELECT id, uid, type, payload, arrival_ns FROM events ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var ev Event
		if err := rows.Scan(&ev.ID, &ev.UID, &ev.Type, &ev.Payload, &ev.ArrivalNs); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// Has 报告指定幂等键的事件是否已存在于日志中。
func (s *Store) Has(uid string) (bool, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM events WHERE uid = ?`, uid).Scan(&n)
	return n > 0, err
}

// Len 返回日志中的事件数。
func (s *Store) Len() (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n)
	return n, err
}

func (s *Store) Close() error { return s.db.Close() }
