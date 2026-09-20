// lifesupportd 是载人舱生命保障告警服务。
//
// 用法：
//
//	lifesupportd -db /var/lib/lifesupport/alerts.db -addr :50051 -heartbeat 10s
//
// 启动时若库中无任何阈值版本，注册内置的在轨默认配置，保证开箱可用；
// 已有配置则原样加载（事件日志为唯一事实来源，重启不丢状态）。
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	pb "lifesupport/gen/lifesupport/v1"
	"lifesupport/internal/core"
	"lifesupport/internal/service"
	"lifesupport/internal/store"
)

func main() {
	var (
		dbPath    = flag.String("db", "lifesupport.db", "SQLite 数据库路径")
		addr      = flag.String("addr", ":50051", "gRPC 监听地址")
		heartbeat = flag.Duration("heartbeat", 10*time.Second, "升级心跳间隔（地面时钟推进）")
	)
	flag.Parse()

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("打开存储失败: %v", err)
	}
	defer st.Close()

	// 首次启动注册默认阈值版本；已存在则跳过（版本不可变）。
	cfg, err := st.CurrentConfig()
	if err != nil {
		log.Fatalf("读取当前阈值版本失败: %v", err)
	}
	if cfg == nil {
		def := core.DefaultThresholdVersion()
		if err := st.RegisterThresholdVersion(def, time.Now().UnixNano()); err != nil {
			log.Fatalf("注册默认阈值版本失败: %v", err)
		}
		log.Printf("已注册默认阈值版本 %s（%d 条规则）", def.Version, len(def.Rules))
	} else {
		log.Printf("当前阈值版本 %s", cfg.Version)
	}

	srv := service.NewServer(st, nil)
	stop := srv.StartHeartbeat(*heartbeat)
	defer stop()

	gs := grpc.NewServer()
	pb.RegisterLifeSupportAlertsServer(gs, srv)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("监听失败: %v", err)
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Printf("收到退出信号，优雅停止（状态已持久化，可随时重放恢复）")
		gs.GracefulStop()
	}()

	log.Printf("生命保障告警服务监听于 %s，数据库 %s", *addr, *dbPath)
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("服务退出: %v", err)
	}
}
