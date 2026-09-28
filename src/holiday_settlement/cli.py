"""结算协同服务命令行：端到端演示、重启重放、勾稽校验与溯源。

用法：
    python3 -m src.holiday_settlement.cli demo [--dir DATA_DIR]
    python3 -m src.holiday_settlement.cli verify DATA_DIR
    python3 -m src.holiday_settlement.cli trace DATA_DIR CAMPAIGN_ID [MERCHANT_ID]
"""

from __future__ import annotations

import argparse
import datetime as dt
import shutil
from decimal import Decimal
from pathlib import Path

from .model import (
    BatchSpec, BudgetSpec, CampaignSpec, PaymentData, RedemptionData,
    RefundData, Role, User,
)
from .service import SettlementService

ORG = User("活动主办方·文旅科", (Role.ORGANIZER,))
FIN = User("财务·结算科", (Role.FINANCE,))
AUD = User("审计·监督科", (Role.AUDITOR,))
OPS = User("商圈运营值班员", (Role.FINANCE, Role.ORGANIZER, Role.AUDITOR))

D = Decimal
SEAL_DAY = dt.date(2026, 10, 8)


def _line(title: str = "") -> None:
    print(("\n" + "═" * 68 + "\n " + title) if title else "")


def _money(value) -> str:
    return f"{Decimal(str(value)):.2f}"


def run_demo(data_dir: Path, *, restart: bool = False) -> SettlementService:
    """品牌快闪 / 夜游 / 餐饮节三类活动的完整协同结算演示。"""
    fresh = not (data_dir / "events.log.jsonl").exists()
    svc = SettlementService.load(data_dir, today=SEAL_DAY)
    if not fresh:
        return svc

    print("步骤 1  主办方维护活动规则与券批次，财务/审计不可改规则")
    svc.register_budget(ORG, BudgetSpec("ANNUAL-2026", D("2000000")))
    svc.register_campaign(ORG, CampaignSpec(
        "POPUP", "品牌快闪周", D("0.20"), SEAL_DAY,
        budget_id="ANNUAL-2026", max_subsidy_per_order=D("200"),
        merchants=frozenset({"M-潮牌", "M-美妆"})))
    svc.register_campaign(ORG, CampaignSpec(
        "NIGHT", "中秋夜游季", D("0.30"), SEAL_DAY,
        budget_id="ANNUAL-2026",
        merchants=frozenset({"M-游船", "M-灯会", "M-潮牌"})))
    svc.register_campaign(ORG, CampaignSpec(
        "FOOD", "金秋餐饮节", D("0.25"), SEAL_DAY,
        standalone_budget=D("300000"), max_subsidy_per_order=D("150"),
        merchants=frozenset({"M-烤鸭", "M-火锅"})))
    svc.register_batch(ORG, BatchSpec("POPUP-Q1", "POPUP", frozenset({"PP-001", "PP-002"}), D("80000")))
    svc.register_batch(ORG, BatchSpec("NIGHT-Q1", "NIGHT", frozenset({"NT-001", "NT-002"}), D("120000")))
    svc.register_batch(ORG, BatchSpec("FOOD-Q1", "FOOD", frozenset({"FD-001", "FD-002", "FD-003"}), D("90000")))
    for cid in ("POPUP", "NIGHT", "FOOD"):
        print(f"  · {cid} 规则就绪：{svc.splits(cid)['rule']['rule_explanation']}")

    _line("步骤 2  数据分批、乱序到达：先到核销与退款先挂起，不丢单")
    # 夜游核销先于银联支付 -> await_payment
    print("  ->", svc.ingest_redemption(OPS, RedemptionData(
        "ch1-rd-01", "RD-1001", "NIGHT", "M-游船", "UPI-5001", "NT-001",
        D("360"), {"项目": "夜游船票×2", "时段": "20:00"}, dt.date(2026, 9, 26))))
    # 退款回执最先到 -> await_order
    print("  ->", svc.ingest_refund(OPS, RefundData(
        "ch2-rf-09", "RF-9009", "UPI-5002", D("80"), "M-火锅",
        dt.date(2026, 9, 27))))
    print("  挂起队列：", [(p["record_id"], p["wait"]) for p in svc.list_pending()])

    _line("步骤 3  银联支付记录到达，挂起单据自动消化")
    svc.ingest_payment(OPS, PaymentData(
        "ch1-py-01", "UPI-5001", "M-游船", D("360"), dt.date(2026, 9, 26)))
    print("  夜游 RD-1001：", svc.get_order("RD-1001")["status"],
          "冻结补贴", _money(svc.get_order("RD-1001")["frozen"]))

    _line("步骤 4  同记录号重传：金额相同、订单内容不同 -> 隔离，不覆盖原记录")
    svc.ingest_payment(OPS, PaymentData(
        "ch3-py-01", "UPI-5003", "M-潮牌", D("800"), dt.date(2026, 9, 28)))
    svc.ingest_redemption(OPS, RedemptionData(
        "ch3-rd-01", "RD-1002", "POPUP", "M-潮牌", "UPI-5003", "PP-001",
        D("800"), {"商品": "联名卫衣（白色 M）"}, dt.date(2026, 9, 28)))
    again = svc.ingest_redemption(OPS, RedemptionData(
        "ch3-rd-01", "RD-1002", "POPUP", "M-潮牌", "UPI-5003", "PP-001",
        D("800"), {"商品": "联名卫衣（黑色 L，疑似换单）"}, dt.date(2026, 9, 28)))
    print("  重传结果：", again)
    q = svc.list_quarantine()[0]
    print("  隔离原因：", q["reason"], "；原记录指纹保留，受理不受影响：",
          svc.get_order("RD-1002")["status"])

    _line("步骤 5  同一笔消费跨活动申报第二次补贴 -> 直接拒绝")
    second = svc.ingest_redemption(OPS, RedemptionData(
        "ch3-rd-02", "RD-1003", "NIGHT", "M-潮牌", "UPI-5003", "NT-002",
        D("800"), {"项目": "同店加报夜游补贴"}, dt.date(2026, 9, 28)))
    print("  第二次补贴申报：", second,
          "| 拒绝原因：", svc.list_rejected()[0]["reason"])

    _line("步骤 6  餐饮节正常核销 + 跨店退款争议：只有审计能解锁")
    svc.ingest_payment(OPS, PaymentData(
        "ch2-py-02", "UPI-5002", "M-火锅", D("400"), dt.date(2026, 9, 25)))
    svc.ingest_redemption(OPS, RedemptionData(
        "ch2-rd-02", "RD-2002", "FOOD", "M-火锅", "UPI-5002", "FD-001",
        D("400"), {"桌号": "A12"}, dt.date(2026, 9, 25)))
    # 步骤 2 中 M-火锅 自己的退款此时自动落账（400 的 1/5 -> 冲回补贴 25）
    o = svc.get_order("RD-2002")
    print(f"  RD-2002 补贴 {_money(o['subsidy'])}，早到退款冲回后冻结 {_money(o['frozen'])}")
    # 跨店退款：退款由 M-烤鸭（同商场另一门店）发起
    cross = svc.ingest_refund(OPS, RefundData(
        "ch2-rf-10", "RF-9010", "UPI-5002", D("120"), "M-烤鸭",
        dt.date(2026, 9, 29)))
    print("  跨店退款：", cross, "-> 争议挂起，补贴款项不动")
    dispute = next(d for d in svc.list_open_disputes() if d["kind"] == "cross_store_refund")
    print("  争议单：", dispute["dispute_id"], "|", dispute["reason"])
    svc.resolve_dispute(AUD, dispute["dispute_id"], {
        "decision": "apply_refund",
        "responsible_merchant": "M-烤鸭",
        "依据": "退款由 M-烤鸭 柜台发起且签购单为其门店码，补贴返还责任归 M-烤鸭",
    })
    o = svc.get_order("RD-2002")
    print(f"  审计解锁后：承担企业 M-烤鸭，追加冲回，冻结净额 {_money(o['frozen'])}")

    _line("步骤 7  结算窗口封账（财务角色）：冻结转已结算，快照落盘不可改")
    for cid in ("POPUP", "NIGHT", "FOOD"):
        snap = svc.seal_window(FIN, cid)
        total = sum((Decimal(str(m["subsidy"])) for m in snap["merchants"]), D("0"))
        print(f"  {cid} 快照 {snap['snapshot_id']}："
              f"{len(snap['merchants'])} 家商户分账，补贴合计 {_money(total)}")

    _line("步骤 8  封账后商户补报：不改快照，财务确认后形成后期调整")
    svc.ingest_payment(OPS, PaymentData(
        "late-py-1", "UPI-5099", "M-灯会", D("500"), dt.date(2026, 10, 2)))
    svc.ingest_redemption(OPS, RedemptionData(
        "late-rd-1", "RD-9001", "NIGHT", "M-灯会", "UPI-5099", "NT-002",
        D("500"), {"项目": "夜游补报：灯彩展"}, dt.date(2026, 10, 2)))
    svc.confirm_late_order(FIN, "RD-9001", note="商户网络故障补报，银联流水已核对")
    print("  RD-9001 后期补付：", _money(svc.get_order("RD-9001")["settled"]),
          "（独立于快照，单独拨付）")

    _line("步骤 9  封账后退款：快照数字不动，记应返还调整并锁定承担企业")
    svc.ingest_refund(OPS, RefundData(
        "late-rf-1", "RF-9099", "UPI-5099", D("500"), "M-灯会",
        dt.date(2026, 10, 12)))
    adj = svc.splits("NIGHT")["post_seal_adjustments"][-1]
    print(f"  {adj['type']}：{adj['merchant_id']} 应返还 {_money(adj['recovered'])} 元")

    _line("步骤 10  额度勾稽（预算池/活动/批次/订单四层）与事件链校验")
    report = svc.reconcile()
    print("  勾稽结果：", "全部吻合" if report["passed"] else "存在差异")
    print("  事件哈希链：", "完整" if report["event_chain_intact"] else "异常")
    pool = svc.budget_view("ANNUAL-2026")
    print(f"  年度预算池：总额 {_money(pool['total'])}，冻结 {_money(pool['frozen'])}，"
          f"已结算 {_money(pool['settled'])}，后期 {_money(pool['late_settled'])}，"
          f"应返还 {_money(pool['recovered'])}，可用 {_money(pool['available'])}")
    return svc


def cmd_demo(args) -> None:
    data = Path(args.dir)
    if args.fresh and data.exists():
        shutil.rmtree(data)
    run_demo(data)
    print("\n演示数据目录：", data)
    print("可用 verify / trace 子命令复查，或再次运行 demo 查看重启重放结果。")


def cmd_verify(args) -> None:
    svc = SettlementService.load(Path(args.dir))
    report = svc.reconcile()
    for check in report["checks"]:
        mark = "✓" if check["passed"] else "✗"
        print(f"{mark} {check['check']}")
        if not check["passed"]:
            print("   ", check["detail"])
    print("事件哈希链：", "完整" if report["event_chain_intact"] else "异常")
    raise SystemExit(0 if report["passed"] and report["event_chain_intact"] else 1)


def cmd_trace(args) -> None:
    svc = SettlementService.load(Path(args.dir))
    report = svc.trace(args.campaign, args.merchant)
    snap = report["snapshot"]
    print(f"活动 {args.campaign} 汇总：已结算 {_money(report['campaign_view']['settled'])}，"
          f"后期 {_money(report['campaign_view']['late_settled'])}，"
          f"应返还 {_money(report['campaign_view']['recovered'])}")
    if snap:
        print(f"封账快照：{snap['snapshot_id']}（指纹 {snap['fingerprint'][:16]}…）")
    for line in report["merchants"]:
        if line["source"] == "window":
            print(f"\n[快照分账行] {line['merchant_id']} 补贴 {_money(line['subsidy'])}")
            for o in line["orders"]:
                print(f"    核销 {o['redemption_id']}（记录 {o['record_id']}）"
                      f" 消费 {_money(o['amount'])} 补贴 {_money(o['subsidy'])}"
                      f" 状态 {o['status']} 事件#{o['accepted_seq']}")
            for pay in line["payments"]:
                print(f"    └ 银联支付 {pay['payment_id']} {_money(pay['amount'])}")
            for rf in line["refunds"]:
                print(f"    └ 退款 {rf['refund_id']} {_money(rf['amount'])}"
                      f" 承担方 {rf.get('responsible_merchant', '-')}"
                      f" 状态 {rf['status']}")
        else:
            amount = line.get("subsidy") or line.get("recovered")
            print(f"\n[封账后调整] {line['type']} {line['merchant_id']} "
                  f"{_money(amount)} 元（事件#{line['event_seq']}，{line['note']}）")
            ev = line.get("evidence", {})
            if ev.get("order"):
                o = ev["order"]
                print(f"    核销 {o['redemption_id']}（记录 {o['record_id']}）"
                      f" 消费 {_money(o['amount'])} 补贴 {_money(o['subsidy'])}")
            if ev.get("payment"):
                print(f"    └ 银联支付 {ev['payment']['payment_id']} "
                      f"{_money(ev['payment']['amount'])}")


def main() -> None:
    parser = argparse.ArgumentParser(description="节日商圈结算协同服务")
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo", help="运行端到端演示")
    demo.add_argument("--dir", default=".settlement-demo", help="事件日志目录")
    demo.add_argument("--fresh", action="store_true", help="清空目录后重新演示")
    demo.set_defaults(func=cmd_demo)

    verify = sub.add_parser("verify", help="勾稽与哈希链校验")
    verify.add_argument("dir")
    verify.set_defaults(func=cmd_verify)

    trace = sub.add_parser("trace", help="从汇总数字追溯原始记录")
    trace.add_argument("dir")
    trace.add_argument("campaign")
    trace.add_argument("merchant", nargs="?", default=None)
    trace.set_defaults(func=cmd_trace)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
