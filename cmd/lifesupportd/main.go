// lifesupportd 生命保障告警服务守护进程。
//
// 用法:
//
//	lifesupportd -db /var/lib/lifesupport/events.db -addr :50051
//
// 首次启动 (日志为空) 时写入在轨默认阈值版本与 ON_ORBIT 任务阶段;
// 此后重启只做日志重放, 不重复播种。
package main

import (
	"flag"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	lsv1 "lifesupport/gen/lifesupport/v1"
	"lifesupport/internal/engine"
	"lifesupport/internal/server"
)

func main() {
	dbPath := flag.String("db", "lifesupport.db", "SQLite 事件日志路径")
	addr := flag.String("addr", ":50051", "gRPC 监听地址")
	flag.Parse()

	now := func() int64 { return time.Now().UnixNano() }
	eng, err := engine.Open(*dbPath, engine.DefaultConfig(now))
	if err != nil {
		log.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	if err := seedIfEmpty(eng, now); err != nil {
		log.Fatalf("seed: %v", err)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	lsv1.RegisterLifeSupportServer(gs, server.New(eng, now))
	log.Printf("lifesupportd: db=%s addr=%s", *dbPath, *addr)
	log.Fatal(gs.Serve(lis))
}

// seedIfEmpty 在空日志上播种默认阈值与任务阶段 (幂等: 仅当无事件时执行)。
func seedIfEmpty(eng *engine.Engine, now func() int64) error {
	n, err := eng.EventCount()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil // 已有事件 (含重启重放), 不重复播种
	}
	if _, err := eng.Apply(engine.EvThresholds, engine.DefaultThresholds(0), "seed:thresholds:v1-onorbit", now()); err != nil {
		return err
	}
	_, err = eng.Apply(engine.EvPhase, &lsv1.PhaseChangedEvent{
		Phase:                 lsv1.MissionPhase_MISSION_PHASE_ON_ORBIT,
		EffectiveFromDeviceNs: 0,
		SetBy:                 "system-seed",
	}, "seed:phase:on-orbit", now())
	return err
}
