"""节日商圈结算协同服务：端到端业务叙事演示。

运行：python3 -m examples.demo
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from src.holiday_settlement import (
    AUDITOR,
    FINANCE,
    OPERATOR,
    ORGANIZER,
    Principal,
    SettlementService,
    to_cents,
    to_yuan,
)
from src.holiday_settlement.errors import RuleConflictError, SettlementError

OP = Principal("商圈运营-小林", OPERATOR)
FIN = Principal("财务-周姐", FINANCE)
ORG = Principal("活动主办方-老陈", ORGANIZER)
AUD = Principal("审计-赵工", AUDITOR)


def show(title: str) -> None:
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


def pool(svc, pid="Y2026") -> None:
    v = svc.pool_view(pid)
    print(
        f"  池 {pid}：总额 {to_yuan(v['total_cents'])} | 可用 {to_yuan(v['available_cents'])} "
        f"| 冻结 {to_yuan(v['frozen_cents'])} | 已结算 {to_yuan(v['settled_cents'])} "
        f"| 恒等式 {v['identity_total_check']}"
    )


def main() -> None:
    tmp = tempfile.TemporaryDirectory()
    journal = Path(tmp.name) / "festival.jsonl"

    show("1. 建档与规则（运营收企业，财务存预算，主办方定规则）")
    svc = SettlementService(journal)
    svc.execute("register_company", "c1", {"company_id": "C1", "name": "老字号烤鸭店"}, OP)
    svc.execute("register_company", "c2", {"company_id": "C2", "name": "夜游光影公司"}, OP)
    svc.execute("register_company", "c3", {"company_id": "C3", "name": "快闪品牌方"}, OP)
    # 品牌快闪、夜游、餐饮节三个活动共享同一年度预算池
    svc.execute("deposit_budget", "dep1",
                {"pool_id": "Y2026", "amount_cents": to_cents("500000"), "year": 2026}, FIN)
    svc.execute("define_activity", "a1",
                {"activity_id": "POPUP", "name": "品牌快闪", "pool_id": "Y2026",
                 "ratio_permille": 1000, "per_order_cap_cents": to_cents("20")}, ORG)
    svc.execute("define_activity", "a2",
                {"activity_id": "NIGHT", "name": "夜游项目", "pool_id": "Y2026",
                 "ratio_permille": 500, "per_order_cap_cents": to_cents("50")}, ORG)
    svc.execute("define_activity", "a3",
                {"activity_id": "FOOD", "name": "中秋餐饮节", "pool_id": "Y2026",
                 "ratio_permille": 300}, ORG)
    svc.execute("define_coupon_batch", "q1",
                {"batch_id": "B-POP", "activity_id": "POPUP",
                 "face_value_cents": to_cents("20"), "category": "popup"}, ORG)
    svc.execute("define_coupon_batch", "q2",
                {"batch_id": "B-NIGHT", "activity_id": "NIGHT", "category": "night"}, ORG)
    svc.execute("open_window", "w1",
                {"period_id": "P1", "activity_ids": ["POPUP", "NIGHT", "FOOD"]}, OP)
    print("  三个活动共享年度预算池 Y2026（50 万元）")

    show("2. 数据分批、乱序到达：核销先冻结，支付后匹配")
    svc.execute("receive_redemption", "r1",
                {"order_id": "O1", "company_id": "C1", "batch_id": "B-POP",
                 "code": "CP-0001", "gross_cents": to_cents("268"),
                 "period_id": "P1", "source": "brand", "channel_ref": "BR-0001"}, OP)
    pool(svc)
    # 支付回执晚到
    svc.execute("receive_payment", "u1",
                {"order_id": "O1", "company_id": "C1", "payment_reference": "UP-9001",
                 "amount_cents": to_cents("268"), "period_id": "P1",
                 "source": "unionpay", "channel_ref": "UP-9001"}, OP)
    print("  银联支付 UP-9001 晚到，订单 O1 完成匹配进入可结算")

    show("3. 一笔消费不能拿两次补贴（夜游券试图复用同一笔银联支付）")
    try:
        svc.execute("receive_redemption", "r2",
                    {"order_id": "O2", "company_id": "C2", "batch_id": "B-NIGHT",
                     "code": "NT-0001", "gross_cents": to_cents("300"),
                     "payment_reference": "UP-9001", "period_id": "P1",
                     "source": "brand", "channel_ref": "BR-0002"}, OP)
    except RuleConflictError as exc:
        print(f"  已拦截：{exc}")
    # 合法的夜游订单继续
    svc.execute("receive_redemption", "r3",
                {"order_id": "O3", "company_id": "C2", "batch_id": "B-NIGHT",
                 "code": "NT-0002", "gross_cents": to_cents("300"),
                 "period_id": "P1", "source": "brand", "channel_ref": "BR-0003"}, OP)
    svc.execute("receive_payment", "u3",
                {"order_id": "O3", "company_id": "C2", "payment_reference": "UP-9003",
                 "amount_cents": to_cents("300"), "period_id": "P1",
                 "source": "unionpay", "channel_ref": "UP-9003"}, OP)

    show("4. 渠道重传：同内容吸收；金额相同、订单不同则隔离")
    same = {"order_id": "O1", "company_id": "C1", "batch_id": "B-POP",
            "code": "CP-0001", "gross_cents": to_cents("268"),
            "period_id": "P1", "source": "brand", "channel_ref": "BR-0001"}
    r = svc.execute("receive_redemption", "r1-retry", same, OP)
    print(f"  完全相同的重传 -> duplicated={r.duplicated}，额度不重复冻结")
    fake = dict(same, order_id="O8", code="CP-0008")
    r = svc.execute("receive_redemption", "r1-fake", fake, OP)
    print(f"  同流水号不同订单 -> 隔离={r.quarantined}，原因："
          f"{svc.quarantine_list()[-1]['reason']}")

    show("5. 财务在结算窗口生成分账（含可解释分录）")
    svc.execute("settle_window", "s1", {"period_id": "P1"}, FIN)
    item = svc.explain_entry("ST-P1", "O1")
    print("  单笔分账解释：")
    print(f"    {item['entry']['explanation']}")
    print(f"    原始事件：核销 #{item['redemption_event_seq']}，"
          f"支付 #{item['payment_event_seq']}")
    svc.execute("close_books", "x1", {"period_id": "P1"}, FIN)
    pool(svc)
    print("  P1 已封账，快照 SNAP-P1 定格")

    show("6. 封账后补报不能改写快照；跨店退款生成下期追减调整")
    late = svc.execute("receive_redemption", "r-late",
                       {"order_id": "O9", "company_id": "C3", "batch_id": "B-POP",
                        "code": "CP-0009", "gross_cents": to_cents("100"),
                        "period_id": "P1", "source": "brand",
                        "channel_ref": "BR-0009"}, OP)
    print(f"  商户补报 P1 历史核销 -> 隔离={late.quarantined}，SNAP-P1 不变")
    rf = svc.execute("receive_refund", "f1",
                     {"order_id": "O1", "refund_id": "RF-1",
                      "amount_cents": to_cents("268"), "company_id": "C3",
                      "source": "brand", "channel_ref": "RF-0001"}, OP)
    info = rf.events[0].payload
    print(f"  跨店退款：发起方 {info['refunding_company_id']}，"
          f"返还承担方 {info['responsible_company_id']}")
    print(f"  调整单 {rf.events[1].payload['adjustment_id']} 挂下期追减，"
          f"快照分文不动")
    pool(svc)

    show("7. 争议单只有审计能解锁（这里改派返还责任）")
    svc.execute("raise_dispute", "d1",
                {"dispute_id": "D1", "order_id": "O3", "reason": "券码归属两店有争议"}, OP)
    try:
        svc.execute("resolve_dispute", "d-x",
                    {"dispute_id": "D1", "decision": "release"}, OP)
    except SettlementError as exc:
        print(f"  运营解锁被拒：{exc}")
    svc.execute("resolve_dispute", "d2",
                {"dispute_id": "D1", "decision": "reassign",
                 "new_company_id": "C3", "note": "实际由快闪品牌承担返还"}, AUD)
    print("  审计已解锁：返还责任改派至 C3，快照不变，下期调整单解释")

    show("8. 服务重启：重放哈希链，额度/退款/已结算重新勾稽")
    del svc
    svc = SettlementService(journal)
    result = svc.reconcile()
    print(f"  重放 {svc.head_seq} 条事件，链头 {svc.head_digest[:12]}...")
    print(f"  全局对账 ok={result['ok']}，问题={result['problems']}")
    print(f"  订单状态分布：{result['orders_by_state']}")

    show("9. 从汇总数字一路钻取到原始核销")
    traced = svc.trace_settlement("ST-P1")
    c1 = traced["settlement"]["totals_company"]["C1"]
    print(f"  ST-P1 企业 C1 汇总：{c1['orders']} 笔，"
          f"补贴 {to_yuan(c1['subsidy_cents'])} 元")
    print(f"  {traced['drilldown']}")
    timeline = svc.trace_order("O1")["timeline"]
    print("  O1 每次调整的事件时间线：")
    for step in timeline:
        print(f"    #{step['event_seq']:>3}  {step['step']}")

    tmp.cleanup()


if __name__ == "__main__":
    main()
