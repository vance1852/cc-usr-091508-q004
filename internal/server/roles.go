package server

import (
	"google.golang.org/protobuf/proto"

	lsv1 "lifesupport/gen/lifesupport/v1"
)

// ---------------------------------------------------------------------------
// 角色字段裁剪: 乘组、飞控、医学支持分别只取得履职所需字段。
//
//   乘组:   告警是什么、该执行哪条程序、当前状态; 不给原始采样流、
//           阈值版本与值班人员姓名, 避免与飞控指令相互矛盾。
//   医学:   暴露相关的全部生理参数 (采样明细、峰值、持续区间), 不给
//           升级链路与人员姓名。
//   飞控:   全部字段。
// ---------------------------------------------------------------------------

// MaskAlertFor 返回按角色裁剪后的告警视图副本 (不修改原对象)。
func MaskAlertFor(role lsv1.Role, in *lsv1.AlertView) *lsv1.AlertView {
	if in == nil {
		return nil
	}
	v := proto.Clone(in).(*lsv1.AlertView)
	switch role {
	case lsv1.Role_ROLE_FLIGHT_CONTROL:
		// 全量
	case lsv1.Role_ROLE_MEDICAL:
		v.ThresholdVersion = ""
		v.Owner = ""
		v.AckedBy = ""
		v.ResolvedBy = ""
		v.EscalationHistory = nil
		v.NextEscalationNs = 0
		v.NsToNextEscalation = 0
		maskEvidenceNames(v)
	case lsv1.Role_ROLE_CREW:
		v.ThresholdVersion = ""
		v.ContributingSamples = nil
		v.Owner = ""
		v.AckedBy = ""
		v.ResolvedBy = ""
		v.EscalationHistory = nil
		v.NextEscalationNs = 0
		v.NsToNextEscalation = 0
		v.PeakValue = 0
		v.LastValue = 0
		maskEvidenceNames(v)
	default: // 未指定角色按最保守处理
		return MaskAlertFor(lsv1.Role_ROLE_CREW, in)
	}
	return v
}

func maskEvidenceNames(v *lsv1.AlertView) {
	for _, ev := range v.Evidence {
		ev.Operator = ""
	}
}

// MaskHandoverFor 裁剪交班视图中的每条告警; 传感器组状态 (含证据不足)
// 对所有角色可见 —— 任何角色都不应把失联误判为安全。
func MaskHandoverFor(role lsv1.Role, in *lsv1.HandoverView) *lsv1.HandoverView {
	hv := proto.Clone(in).(*lsv1.HandoverView)
	for i, a := range hv.Alerts {
		hv.Alerts[i] = MaskAlertFor(role, a)
	}
	return hv
}

// MaskAlertListFor 裁剪告警列表。
func MaskAlertListFor(role lsv1.Role, in []*lsv1.AlertView) []*lsv1.AlertView {
	out := make([]*lsv1.AlertView, len(in))
	for i, a := range in {
		out[i] = MaskAlertFor(role, a)
	}
	return out
}
