"""结算协同服务测试：覆盖分批接入、乱序、重传隔离、共享预算、
防重复补贴、封账不可变、后期调整、争议审计解锁、重启重放与额度勾稽。"""

from __future__ import annotations

import datetime as dt
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from src.holiday_settlement.model import (
    BatchSpec, BudgetSpec, CampaignSpec, PaymentData, RedemptionData,
    RefundData, Role, User, content_fingerprint,
)
from src.holiday_settlement.service import (
    PermissionDenied, RuleViolation, SettlementService,
)

D = Decimal
TODAY = dt.date(2026, 10, 1)
DAY = dt.date(2026, 9, 20)

FIN = User("财务小周", (Role.FINANCE,))
ORG = User("主办方小吴", (Role.ORGANIZER,))
AUD = User("审计小郑", (Role.AUDITOR,))
OPS = User("运营小冯", (Role.FINANCE, Role.ORGANIZER, Role.AUDITOR))
GUEST = User("访客小李", ())


def redemption(record_id="rec-r1", redemption_id="R1", campaign="C1",
               merchant="M1", payment="P1", code="K1", amount=D("200"),
               content=None, day=DAY) -> RedemptionData:
    return RedemptionData(
        record_id, redemption_id, campaign, merchant, payment, code,
        D(amount), content if content is not None else {"item": "套餐A"}, day)


def payment(record_id="rec-p1", payment="P1", merchant="M1", amount=D("200"),
            day=DAY) -> PaymentData:
    return PaymentData(record_id, payment, merchant, D(amount), day)


def refund(record_id="rec-f1", refund="F1", payment="P1", amount=D("100"),
           merchant="M1", day=DAY + dt.timedelta(days=2)) -> RefundData:
    return RefundData(record_id, refund, payment, D(amount), merchant, day)


class ServiceTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.svc = SettlementService.load(self.dir, today=TODAY)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def setup_campaign(self, campaign="C1", rate_="0.2", cap=None,
                       budget_id="B2026", standalone=None, merchants=("M1", "M2"),
                       settlement_day=TODAY, quota="5000", codes=("K1", "K2", "K3")):
        self.svc.register_budget(ORG, BudgetSpec("B2026", D("100000")))
        self.svc.register_campaign(ORG, CampaignSpec(
            campaign, f"活动-{campaign}", D(rate_), settlement_day,
            budget_id=budget_id, standalone_budget=D(standalone) if standalone else None,
            max_subsidy_per_order=D(cap) if cap else None,
            merchants=frozenset(merchants)))
        self.svc.register_batch(ORG, BatchSpec(
            f"Q-{campaign}", campaign, frozenset(codes), D(quota)))

    def restart(self, today=TODAY) -> SettlementService:
        return SettlementService.load(self.dir, today=today)


class TestRolePermissions(ServiceTestBase):
    def test_finance_cannot_change_rules(self):
        with self.assertRaises(PermissionDenied):
            self.svc.register_budget(FIN, BudgetSpec("B", D("100")))
        with self.assertRaises(PermissionDenied):
            self.svc.register_campaign(FIN, CampaignSpec(
                "C", "c", D("0.2"), TODAY, standalone_budget=D("100")))
        with self.assertRaises(PermissionDenied):
            self.svc.register_batch(FIN, BatchSpec("Q", "C", frozenset({"K"}), D("100")))

    def test_organizer_cannot_seal_window(self):
        self.setup_campaign()
        with self.assertRaises(PermissionDenied):
            self.svc.seal_window(ORG, "C1")

    def test_only_auditor_unlocks_dispute(self):
        self.setup_campaign()
        self.svc.ingest_payment(OPS, payment())
        self.svc.ingest_redemption(OPS, redemption())
        self.svc.ingest_refund(OPS, refund(merchant="M2"))
        dispute = self.svc.list_open_disputes()[0]
        with self.assertRaises(PermissionDenied):
            self.svc.resolve_dispute(FIN, dispute["dispute_id"],
                                     {"decision": "apply_refund"})
        with self.assertRaises(PermissionDenied):
            self.svc.resolve_dispute(ORG, dispute["dispute_id"],
                                     {"decision": "apply_refund"})
        self.svc.resolve_dispute(
            AUD, dispute["dispute_id"],
            {"decision": "apply_refund", "responsible_merchant": "M2"})
        self.assertEqual(self.svc.list_open_disputes(), [])

    def test_guest_can_do_nothing(self):
        with self.assertRaises(PermissionDenied):
            self.svc.register_budget(GUEST, BudgetSpec("B", D("1")))


class TestBudgetAndFreeze(ServiceTestBase):
    def test_redemption_freezes_subsidy(self):
        self.setup_campaign(cap="50")
        self.svc.ingest_payment(OPS, payment(amount="200"))
        self.svc.ingest_redemption(OPS, redemption(amount="200"))
        view = self.svc.campaign_view("C1")
        self.assertEqual(view["frozen"], D("40.00"))
        self.assertEqual(view["settled"], D("0.00"))
        self.assertEqual(view["available"], D("99960.00"))
        order = self.svc.get_order("R1")
        self.assertEqual(order["subsidy"], D("40.00"))
        self.assertIn("20%", order["explanation"]["rule"])
        self.assertEqual(order["explanation"]["calc"], "200.00 × 0.2 = 40.00")

    def test_cap_applied(self):
        self.setup_campaign(cap="30")
        self.svc.ingest_payment(OPS, payment(amount="500"))
        self.svc.ingest_redemption(OPS, redemption(amount="500"))
        self.assertEqual(self.svc.get_order("R1")["subsidy"], D("30.00"))

    def test_batch_quota_blocks_then_releases_after_refund(self):
        # 批次额度只有 50：首单 40，次单需要 40 -> 挂起等额度
        self.setup_campaign(quota="50")
        self.svc.ingest_payment(OPS, payment("p1", "P1", amount="200"))
        self.svc.ingest_redemption(OPS, redemption("r1", "R1", payment="P1", amount="200"))
        self.svc.ingest_payment(OPS, payment("p2", "P2", amount="200"))
        self.assertEqual(
            self.svc.ingest_redemption(
                OPS, redemption("r2", "R2", payment="P2", code="K2", amount="200")),
            "pending")
        wait = self.svc.list_pending()[0]
        self.assertEqual(wait["wait"], "await_quota")
        # 首单全额退款，冻结释放 40，第二单自动受理
        self.svc.ingest_refund(OPS, refund("f1", "F1", payment="P1", amount="200"))
        self.assertEqual(self.svc.get_order("R2")["status"], "accepted")
        self.assertEqual(self.svc.get_order("R2")["subsidy"], D("40.00"))


class TestOutOfOrder(ServiceTestBase):
    def test_redemption_before_payment(self):
        self.setup_campaign()
        self.assertEqual(self.svc.ingest_redemption(OPS, redemption()), "pending")
        self.assertEqual(self.svc.list_pending()[0]["wait"], "await_payment")
        self.assertEqual(self.svc.ingest_payment(OPS, payment()), "accepted")
        self.assertEqual(self.svc.get_order("R1")["status"], "accepted")
        self.assertEqual(self.svc.list_pending(), [])

    def test_refund_before_order_then_chain_resolves(self):
        self.setup_campaign()
        # 退款最先到
        self.assertEqual(self.svc.ingest_refund(OPS, refund()), "pending")
        # 核销其次（仍无支付）
        self.assertEqual(self.svc.ingest_redemption(OPS, redemption()), "pending")
        # 支付最后到：核销受理 -> 同店退款自动落账
        self.svc.ingest_payment(OPS, payment())
        order = self.svc.get_order("R1")
        self.assertEqual(order["frozen"], D("20.00"))      # 40 补贴冲回一半
        self.assertEqual(order["refunded_amount"], D("100.00"))
        self.assertEqual(order["recovered"], D("20.00"))

    def test_duplicate_delivery_is_idempotent(self):
        self.setup_campaign()
        self.svc.ingest_payment(OPS, payment())
        self.svc.ingest_redemption(OPS, redemption())
        self.assertEqual(self.svc.ingest_payment(OPS, payment()), "ignored_duplicate")
        self.assertEqual(self.svc.ingest_redemption(OPS, redemption()), "ignored_duplicate")
        self.assertEqual(len(self.svc.orders), 1)


class TestRetransmitIsolation(ServiceTestBase):
    def test_same_amount_different_content_is_quarantined(self):
        self.setup_campaign()
        self.svc.ingest_redemption(OPS, redemption(content={"item": "套餐A"}))
        # 渠道重传：记录号、金额完全相同，但订单内容不同
        status = self.svc.ingest_redemption(
            OPS, redemption(content={"item": "套餐B（疑似换单）"}))
        self.assertEqual(status, "quarantined")
        quarantined = self.svc.list_quarantine()
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0]["reason"], "fingerprint_conflict")
        self.assertIn("original_record_id", quarantined[0])
        # 原始记录仍然有效，支付到达后按原内容受理
        self.svc.ingest_payment(OPS, payment())
        self.assertEqual(self.svc.get_order("R1")["fingerprint"],
                         quarantined[0]["detail"]["original_fingerprint"])

    def test_payment_retransmit_with_changed_amount_isolated(self):
        self.setup_campaign()
        self.svc.ingest_payment(OPS, payment(amount="200"))
        status = self.svc.ingest_payment(OPS, payment(amount="220"))
        self.assertEqual(status, "quarantined")
        self.assertEqual(self.svc.payments["P1"].amount, D("200.00"))


class TestDuplicateSubsidy(ServiceTestBase):
    def test_shared_pool_same_payment_second_subsidy_rejected(self):
        self.setup_campaign(campaign="C1", rate_="0.2")
        self.svc.register_campaign(ORG, CampaignSpec(
            "C2", "夜游", D("0.3"), TODAY, budget_id="B2026"))
        self.svc.register_batch(ORG, BatchSpec(
            "Q-C2", "C2", frozenset({"N1"}), D("5000")))
        self.svc.ingest_payment(OPS, payment())
        self.assertEqual(
            self.svc.ingest_redemption(
                OPS, redemption(campaign="C1", code="K1")), "accepted")
        status = self.svc.ingest_redemption(
            OPS, redemption(record_id="rec-r2", redemption_id="R2",
                            campaign="C2", code="N1"))
        self.assertEqual(status, "rejected")
        rejected = self.svc.list_rejected()
        self.assertEqual(rejected[0]["reason"], "duplicate_subsidy")
        self.assertEqual(rejected[0]["detail"]["owner_campaign"], "C1")
        pool = self.svc.budget_view("B2026")
        self.assertEqual(pool["frozen"], D("40.00"))   # 没有第二笔补贴

    def test_shared_pool_aggregates_across_campaigns(self):
        self.setup_campaign(campaign="C1")
        self.svc.register_campaign(ORG, CampaignSpec(
            "C2", "夜游", D("0.5"), TODAY, budget_id="B2026"))
        self.svc.register_batch(ORG, BatchSpec(
            "Q-C2", "C2", frozenset({"N1", "N2"}), D("90000")))
        self.svc.ingest_payment(OPS, payment("p1", "P1", amount="200"))
        self.svc.ingest_redemption(OPS, redemption(payment="P1", code="K1"))
        self.svc.ingest_payment(OPS, payment("p2", "P2", amount="100"))
        self.svc.ingest_redemption(
            OPS, redemption(record_id="rec-r2", redemption_id="R2",
                            campaign="C2", payment="P2", code="N1", amount="100"))
        pool = self.svc.budget_view("B2026")
        self.assertEqual(pool["frozen"], D("90.00"))   # 40 + 50
        self.assertEqual(sorted(pool["campaigns"]), ["C1", "C2"])


class TestCouponReuse(ServiceTestBase):
    def test_same_coupon_code_cannot_redeem_twice(self):
        self.setup_campaign()
        self.svc.ingest_payment(OPS, payment("p1", "P1", amount="200"))
        self.svc.ingest_redemption(OPS, redemption("r1", "R1", payment="P1", code="K1"))
        self.svc.ingest_payment(OPS, payment("p2", "P2", merchant="M2", amount="200"))
        status = self.svc.ingest_redemption(
            OPS, redemption("r2", "R2", merchant="M2", payment="P2", code="K1"))
        self.assertEqual(status, "rejected")
        self.assertEqual(self.svc.list_rejected()[0]["reason"],
                         "coupon_already_redeemed")
        # 第二单未获补贴，只有首单冻结
        self.assertEqual(self.svc.campaign_view("C1")["frozen"], D("40.00"))


class TestCrossStoreRefund(ServiceTestBase):
    def _open(self):
        self.setup_campaign()
        self.svc.ingest_payment(OPS, payment(merchant="M1"))
        self.svc.ingest_redemption(OPS, redemption(merchant="M1"))
        self.svc.ingest_refund(OPS, refund(merchant="M2"))
        return self.svc.list_open_disputes()[0]

    def test_cross_store_refund_holds_subsidy(self):
        dispute = self._open()
        self.assertEqual(dispute["kind"], "cross_store_refund")
        order = self.svc.get_order("R1")
        self.assertEqual(order["frozen"], D("40.00"))   # 争议期间不冲账
        self.assertIsNotNone(order["dispute_id"])

    def test_auditor_assigns_responsible_merchant(self):
        dispute = self._open()
        self.svc.resolve_dispute(
            AUD, dispute["dispute_id"],
            {"decision": "apply_refund", "responsible_merchant": "M2"})
        rec = next(r for r in self.svc.refunds.values() if r["refund_id"] == "F1")
        self.assertEqual(rec["responsible_merchant"], "M2")
        self.assertEqual(rec["recovered"], D("20.00"))
        self.assertEqual(self.svc.get_order("R1")["frozen"], D("20.00"))

    def test_auditor_dismiss_keeps_money(self):
        dispute = self._open()
        self.svc.resolve_dispute(AUD, dispute["dispute_id"],
                                 {"decision": "dismiss_refund", "note": "误报退款"})
        rec = self.svc.refunds["F1"]
        self.assertNotIn("recovered", rec)
        self.assertEqual(self.svc.get_order("R1")["frozen"], D("40.00"))


class TestSettlementWindow(ServiceTestBase):
    def test_seal_transfers_frozen_to_settled(self):
        self.setup_campaign()
        self.svc.ingest_payment(OPS, payment(amount="200"))
        self.svc.ingest_redemption(OPS, redemption(amount="200"))
        snap = self.svc.seal_window(FIN, "C1")
        self.assertEqual(snap["merchants"][0]["subsidy"], D("40.00"))
        self.assertEqual(snap["merchants"][0]["gross"], D("200.00"))
        view = self.svc.campaign_view("C1")
        self.assertEqual(view["frozen"], D("0.00"))
        self.assertEqual(view["settled"], D("40.00"))
        self.assertTrue(view["sealed"])

    def test_cannot_seal_before_settlement_day(self):
        self.setup_campaign(settlement_day=dt.date(2026, 10, 10))
        self.svc.ingest_payment(OPS, payment())
        self.svc.ingest_redemption(OPS, redemption())
        with self.assertRaisesRegex(RuleViolation, "未到结算日"):
            self.svc.seal_window(FIN, "C1")

    def test_snapshot_is_immutable(self):
        self.setup_campaign()
        self.svc.ingest_payment(OPS, payment())
        self.svc.ingest_redemption(OPS, redemption())
        snap1 = self.svc.seal_window(FIN, "C1")
        with self.assertRaisesRegex(RuleViolation, "已封账"):
            self.svc.seal_window(FIN, "C1")
        path = self.dir / "snapshots" / f"{snap1['snapshot_id']}.json"
        raw = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(raw["snapshot_id"], snap1["snapshot_id"])
        self.assertIn("snapshot_fingerprint", raw)
        # 文件已存在，存储层拒绝再次写入
        with self.assertRaises(PermissionError):
            self.svc.store.write_snapshot(snap1["snapshot_id"], {"x": 1})

    def test_late_report_cannot_change_snapshot(self):
        self.setup_campaign(cap="50")
        self.svc.ingest_payment(OPS, payment())
        self.svc.ingest_redemption(OPS, redemption())
        before = self.svc.seal_window(FIN, "C1")
        before_subsidy = before["merchants"][0]["subsidy"]

        # 商户封账后补报历史核销
        self.svc.ingest_payment(OPS, payment("p-late", "PL", amount="300"))
        status = self.svc.ingest_redemption(
            OPS, redemption(record_id="r-late", redemption_id="RL",
                            payment="PL", code="K2", amount="300"))
        self.assertEqual(status, "accepted")
        order = self.svc.get_order("RL")
        self.assertEqual(order["status"], "late_pending")
        with self.assertRaises(PermissionDenied):
            self.svc.confirm_late_order(ORG, "RL")
        self.svc.confirm_late_order(FIN, "RL", note="商户补报，银联已对账")
        self.assertEqual(order["status"], "late_pending")  # 本地对象快照
        self.assertEqual(self.svc.get_order("RL")["status"], "late_settled")
        self.assertEqual(self.svc.get_order("RL")["settled"], D("50.00"))

        # 快照内容原封不动
        after = self.svc.snapshot("C1")
        self.assertEqual(after["merchants"][0]["subsidy"], before_subsidy)
        adjustments = self.svc.splits("C1")["post_seal_adjustments"]
        self.assertEqual([a["type"] for a in adjustments], ["late_redemption"])
        view = self.svc.campaign_view("C1")
        self.assertEqual(view["settled"], D("40.00"))
        self.assertEqual(view["late_settled"], D("50.00"))

    def test_refund_after_seal_creates_recovery_adjustment(self):
        self.setup_campaign()
        self.svc.ingest_payment(OPS, payment())
        self.svc.ingest_redemption(OPS, redemption())
        self.svc.seal_window(FIN, "C1")
        self.svc.ingest_refund(OPS, refund(amount="200"))
        order = self.svc.get_order("R1")
        self.assertEqual(order["settled"], D("40.00"))       # 快照已结算不回冲
        self.assertEqual(order["recovered_post_seal"], D("40.00"))
        view = self.svc.campaign_view("C1")
        self.assertEqual(view["recovered"], D("40.00"))
        adj = self.svc.splits("C1")["post_seal_adjustments"][-1]
        self.assertEqual(adj["type"], "refund_recovery")
        self.assertEqual(adj["merchant_id"], "M1")

    def test_disputed_order_excluded_from_snapshot(self):
        self.setup_campaign()
        self.svc.ingest_payment(OPS, payment())
        self.svc.ingest_redemption(OPS, redemption())
        self.svc.ingest_refund(OPS, refund(merchant="M2"))
        snap = self.svc.seal_window(FIN, "C1")
        self.assertEqual(snap["merchants"], [])
        self.assertEqual(snap["held_disputes"], ["R1"])
        # 封账后审计驳回退款，订单仍冻结 -> 财务走补报确认净额
        dispute = self.svc.list_open_disputes()[0]
        self.svc.resolve_dispute(AUD, dispute["dispute_id"],
                                 {"decision": "dismiss_refund"})
        self.assertEqual(self.svc.get_order("R1")["status"], "late_pending")
        self.svc.confirm_late_order(FIN, "R1", note="争议驳回，按原补贴拨付")
        self.assertEqual(self.svc.get_order("R1")["status"], "late_settled")
        self.assertEqual(self.svc.get_order("R1")["settled"], D("40.00"))


class TestDataContradiction(ServiceTestBase):
    def test_amount_mismatch_quarantined_and_disputed(self):
        self.setup_campaign()
        self.svc.ingest_payment(OPS, payment(amount="200"))
        status = self.svc.ingest_redemption(OPS, redemption(amount="180"))
        self.assertEqual(status, "quarantined")
        reasons = {q["reason"] for q in self.svc.list_quarantine()}
        self.assertIn("amount_mismatch", reasons)
        self.assertTrue(self.svc.list_open_disputes())
        self.assertNotIn("R1", self.svc.orders)

    def test_unknown_coupon_quarantined_without_freeze(self):
        self.setup_campaign()
        self.svc.ingest_payment(OPS, payment())
        status = self.svc.ingest_redemption(OPS, redemption(code="ZZZ"))
        self.assertEqual(status, "quarantined")
        self.assertEqual(self.svc.campaign_view("C1")["frozen"], D("0.00"))

    def test_refund_over_order_amount_quarantined(self):
        self.setup_campaign()
        self.svc.ingest_payment(OPS, payment(amount="200"))
        self.svc.ingest_redemption(OPS, redemption(amount="200"))
        status = self.svc.ingest_refund(OPS, refund(amount="250"))
        self.assertEqual(status, "quarantined")
        self.assertTrue(any(
            d["kind"] == "quarantined_record" for d in self.svc.list_open_disputes()))


class TestReplayAndIntegrity(ServiceTestBase):
    def _full_scenario(self):
        self.setup_campaign()
        self.svc.ingest_redemption(OPS, redemption(content={"item": "A"}))
        self.svc.ingest_redemption(
            OPS, redemption(content={"item": "B（换单重传）"}))
        self.svc.ingest_payment(OPS, payment())
        self.svc.ingest_refund(OPS, refund(amount="50"))
        self.svc.seal_window(FIN, "C1")
        self.svc.ingest_payment(OPS, payment("p2", "P2", amount="300"))
        self.svc.ingest_redemption(
            OPS, redemption(record_id="r2", redemption_id="R2",
                            payment="P2", code="K2", amount="300"))
        self.svc.confirm_late_order(FIN, "R2")

    def test_restart_rebuilds_identical_state(self):
        self._full_scenario()
        revived = self.restart()
        self.assertTrue(revived.reconcile()["passed"])
        self.assertEqual(
            revived.campaign_view("C1")["settled"],
            self.svc.campaign_view("C1")["settled"])
        self.assertEqual(
            revived.campaign_view("C1")["late_settled"],
            self.svc.campaign_view("C1")["late_settled"])
        self.assertEqual(len(revived.orders), len(self.svc.orders))
        self.assertEqual(len(revived.adjustments), len(self.svc.adjustments))
        self.assertEqual(len(revived.list_quarantine()),
                         len(self.svc.list_quarantine()))

    def test_restart_after_crash_completes_pending_decision(self):
        # 手工只写"核销已收、支付已收"事件，模拟崩溃在自动受理之前
        self.setup_campaign()
        from src.holiday_settlement.store import to_jsonable
        self.svc.store.append("redemption_received", OPS.name, to_jsonable({
            **self.svc._redemption_payload(redemption()),
            "fingerprint": content_fingerprint(self.svc._redemption_payload(redemption())),
        }))
        self.svc.store.append("payment_received", OPS.name, to_jsonable({
            **self.svc._payment_payload(payment()),
            "fingerprint": content_fingerprint(self.svc._payment_payload(payment())),
        }))
        revived = self.restart()
        self.assertEqual(revived.get_order("R1")["status"], "accepted")
        self.assertEqual(revived.list_pending(), [])

    def test_event_chain_tamper_detected(self):
        self._full_scenario()
        # 未篡改时链校验通过
        self.assertTrue(self.svc.store.verify_chain())
        log = self.dir / "events.log.jsonl"
        lines = log.read_text(encoding="utf-8").splitlines()
        event = json.loads(lines[2])
        event["payload"]["amount"] = "999.00"
        lines[2] = json.dumps(event, ensure_ascii=False, sort_keys=True,
                              separators=(",", ":"))
        log.write_text("\n".join(lines) + "\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            self.restart()
        self.assertFalse(self.svc.store.verify_chain())


class TestTraceability(ServiceTestBase):
    def test_summary_drills_down_to_evidence(self):
        self.setup_campaign()
        self.svc.ingest_payment(OPS, payment(amount="200"))
        self.svc.ingest_redemption(OPS, redemption(amount="200"))
        self.svc.seal_window(FIN, "C1")
        report = self.svc.trace("C1", merchant_id="M1")
        line = report["merchants"][0]
        self.assertEqual(line["source"], "window")
        self.assertEqual(line["subsidy"], D("40.00"))
        self.assertEqual(line["orders"][0]["redemption_id"], "R1")
        self.assertEqual(line["payments"][0]["payment_id"], "P1")
        self.assertEqual(report["snapshot"]["snapshot_id"], "snap-C1-2026-10-01")
        self.assertEqual(len(report["snapshot"]["fingerprint"]), 64)

    def test_late_adjustment_traces_to_original_records(self):
        self.setup_campaign()
        self.svc.seal_window(FIN, "C1")
        self.svc.ingest_payment(OPS, payment("p2", "P2", amount="300"))
        self.svc.ingest_redemption(
            OPS, redemption(record_id="r2", redemption_id="R2",
                            payment="P2", code="K2", amount="300"))
        self.svc.confirm_late_order(FIN, "R2", note="补报")
        report = self.svc.trace("C1")
        late = [m for m in report["merchants"]
                if m["source"] == "post_seal_adjustment"][0]
        self.assertEqual(late["redemption_id"], "R2")
        self.assertEqual(late["evidence"]["payment"]["payment_id"], "P2")
        self.assertEqual(late["evidence"]["order"]["explanation"]["batch_id"], "Q-C1")


if __name__ == "__main__":
    unittest.main()
