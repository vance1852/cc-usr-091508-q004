// lsdemo 连接生命保障告警服务，演示一次完整的交班场景：
// CO2 持续越限形成红警 -> 三角色交班视图 -> 飞控确认接管 ->
// 追加解除证据。采样设备时刻回填 10 分钟以立即满足持续窗口。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "lifesupport/gen/lifesupport/v1"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "服务地址")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()
	cli := pb.NewLifeSupportAlertsClient(conn)
	ctx := context.Background()

	now := time.Now()
	dev0 := now.Add(-10 * time.Minute)

	if _, err := cli.SetMissionPhase(ctx, &pb.SetMissionPhaseRequest{
		Phase: pb.MissionPhase_MISSION_PHASE_ON_ORBIT, EffectiveDeviceTime: timestamppb.New(dev0.Add(-time.Hour)),
	}); err != nil {
		log.Fatalf("设置阶段失败: %v", err)
	}

	// CO2 双通道持续越限（主通道延迟 5s、关联通道延迟 7s 到达）。
	var samples []*pb.Sample
	for i := 0; i < 40; i++ {
		dev := dev0.Add(time.Duration(i) * 10 * time.Second)
		samples = append(samples,
			&pb.Sample{SensorId: "co2_primary", Sequence: int64(i + 1), FrameId: fmt.Sprintf("co2_primary#%d", i+1),
				DeviceTime: timestamppb.New(dev), GroundTime: timestamppb.New(dev.Add(5 * time.Second)), Value: 5.5, Unit: "mmHg"},
			&pb.Sample{SensorId: "co2_secondary", Sequence: int64(i + 1), FrameId: fmt.Sprintf("co2_secondary#%d", i+1),
				DeviceTime: timestamppb.New(dev), GroundTime: timestamppb.New(dev.Add(7 * time.Second)), Value: 5.4, Unit: "mmHg"},
		)
	}
	res, err := cli.IngestSamples(ctx, &pb.IngestSamplesRequest{Samples: samples, SubmissionId: "demo-1"})
	if err != nil {
		log.Fatalf("注入失败: %v", err)
	}
	fmt.Printf("注入完成: 接受 %d 条（重复 %d，冲突 %d）\n\n", res.Accepted, res.Duplicates, res.Conflicts)

	// 三角色的交班视图。
	for _, role := range []pb.Role{pb.Role_ROLE_CREW, pb.Role_ROLE_FLIGHT_CONTROL, pb.Role_ROLE_MEDICAL} {
		view, err := cli.GetHandoverView(ctx, &pb.GetHandoverViewRequest{Role: role})
		if err != nil {
			log.Fatalf("交班视图失败: %v", err)
		}
		fmt.Printf("===== 交班视图（%s）=====\n", role)
		for _, a := range view.Alerts {
			fmt.Printf("  [%s/%s] %s\n", a.Severity, a.State, a.Summary)
			fmt.Printf("    告警ID: %s（规则 %s，阈值版本 %s，阶段 %s）\n", a.AlertId, a.Rule, a.ThresholdVersion, a.MissionPhase)
			if a.CurrentOwner != "" {
				fmt.Printf("    当前接管: %s（%s）\n", a.CurrentOwner, a.OwnerRole)
			} else {
				fmt.Printf("    当前接管: 无（待确认）\n")
			}
			if a.NextEscalationDue != nil {
				fmt.Printf("    距下一次升级: %s（级别 L%d）\n", a.EscalationCountdown.AsDuration().Round(time.Second), a.CurrentEscalationLevel+1)
			}
			if len(a.EvidenceSamples) > 0 {
				first := a.EvidenceSamples[0]
				last := a.EvidenceSamples[len(a.EvidenceSamples)-1]
				fmt.Printf("    证据采样: %d 条，%s ~ %s（峰值越限 %.1f）\n",
					len(a.EvidenceSamples),
					first.DeviceTime.AsTime().Format("15:04:05"), last.DeviceTime.AsTime().Format("15:04:05"),
					first.BreachedLimit)
			}
			if a.Exposure != nil {
				fmt.Printf("    暴露: 峰值 %.2f，越红限时长 %s\n", a.Exposure.MaxValue,
					a.Exposure.DurationAboveCritical.AsDuration().Round(time.Second))
			}
			if a.RecommendedCrewAction != "" {
				fmt.Printf("    乘组处置: %s\n", a.RecommendedCrewAction)
			}
		}
		for _, h := range view.SensorHealth {
			fmt.Printf("  传感器 %s: 静默 %s，漂移 %d 次\n", h.SensorId, h.SilenceFor.AsDuration().Round(time.Second), h.DriftObservations)
		}
		fmt.Println()
	}

	// 飞控确认接管。
	view, _ := cli.GetHandoverView(ctx, &pb.GetHandoverViewRequest{Role: pb.Role_ROLE_FLIGHT_CONTROL})
	id := view.Alerts[0].AlertId
	ack, err := cli.AcknowledgeAlert(ctx, &pb.AcknowledgeAlertRequest{
		AlertId: id, OperatorId: "li-wei", Role: pb.Role_ROLE_FLIGHT_CONTROL, Note: "值班接管，已通知乘组",
	})
	if err != nil {
		log.Fatalf("确认失败: %v", err)
	}
	fmt.Printf("确认接管: 当前接管人 %s（状态 %s）\n", ack.CurrentOwner, ack.State)

	// 已确认的红色告警不可删除。
	if _, err := cli.DeleteAlert(ctx, &pb.DeleteAlertRequest{AlertId: id, OperatorId: "li-wei", Role: pb.Role_ROLE_FLIGHT_CONTROL}); err != nil {
		fmt.Printf("删除被拒绝（符合预期）: %v\n", err)
	}

	// 追加解除证据。
	rres, err := cli.AppendResolutionEvidence(ctx, &pb.AppendResolutionEvidenceRequest{
		AlertId: id, OperatorId: "li-wei", Role: pb.Role_ROLE_FLIGHT_CONTROL,
		Summary: "备用 CO2 清除装置投运，ppCO2 回落至 2.0 mmHg 并稳定",
	})
	if err != nil {
		log.Fatalf("追加解除证据失败: %v", err)
	}
	fmt.Printf("解除: 状态 %s，证据链 %d 条\n", rres.State, rres.EvidenceCount)
}
