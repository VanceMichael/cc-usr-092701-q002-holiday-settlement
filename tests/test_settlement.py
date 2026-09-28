"""结算协同服务端到端测试。

覆盖：分批/乱序到达、额度冻结恒等式、共享年度预算、二次补贴拦截、
重传隔离、封账快照不可变、封账前后退款责任、争议审计解锁、
重启重放、角色权限、汇总数字向原始核销的钻取追溯。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.holiday_settlement import (
    AUDITOR,
    FINANCE,
    OPERATOR,
    ORGANIZER,
    Principal,
    SettlementService,
    to_cents,
)
from src.holiday_settlement.errors import (
    AuthorizationError,
    BudgetExhaustedError,
    ClosedPeriodError,
    DisputeStateError,
    DuplicateCommandError,
    RuleConflictError,
    SettlementError,
)
from src.holiday_settlement.journal import Journal
from src.holiday_settlement.state import SettlementState, fingerprint

OP = Principal("运营甲", OPERATOR)
FIN = Principal("财务乙", FINANCE)
ORG = Principal("主办方丙", ORGANIZER)
AUD = Principal("审计丁", AUDITOR)


def base_world(path: str | Path | None = None) -> SettlementService:
    """两家企业、一个共享年度预算池、两个活动、两个券批次、两个窗口。"""
    svc = SettlementService(path)
    svc.execute("register_company", "c1", {"company_id": "C1", "name": "老字号烤鸭店"}, OP)
    svc.execute("register_company", "c2", {"company_id": "C2", "name": "夜游光影公司"}, OP)
    svc.execute("register_company", "c3", {"company_id": "C3", "name": "商圈快闪品牌"}, OP)
    svc.execute(
        "deposit_budget", "b-year",
        {"pool_id": "Y2026", "amount_cents": to_cents("10000"), "year": 2026}, FIN,
    )
    svc.execute(
        "define_activity", "a-pop",
        {"activity_id": "POPUP", "name": "品牌快闪", "pool_id": "Y2026",
         "ratio_permille": 1000, "per_order_cap_cents": 2000}, ORG,
    )
    svc.execute(
        "define_activity", "a-night",
        {"activity_id": "NIGHT", "name": "夜游项目", "pool_id": "Y2026",
         "ratio_permille": 500, "per_order_cap_cents": 5000}, ORG,
    )
    svc.execute(
        "define_coupon_batch", "b1",
        {"batch_id": "B-POP", "activity_id": "POPUP", "face_value_cents": 2000,
         "category": "popup"}, ORG,
    )
    svc.execute(
        "define_coupon_batch", "b2",
        {"batch_id": "B-NIGHT", "activity_id": "NIGHT", "category": "night"}, ORG,
    )
    svc.execute("open_window", "w1", {"period_id": "P1", "activity_ids": ["POPUP", "NIGHT"]}, OP)
    return svc


def redeem(svc, key, order, company="C1", batch="B-POP", code=None,
           gross=5000, period="P1", source="brand", ref=None, payment_ref=None):
    return svc.execute(
        "receive_redemption", key,
        {"order_id": order, "company_id": company, "batch_id": batch,
         "code": code or f"CODE-{order}", "gross_cents": gross,
         "period_id": period, "source": source,
         "channel_ref": ref or f"R-{key}", "payment_reference": payment_ref}, OP,
    )


def pay(svc, key, order, pref, amount=5000, company="C1", period="P1",
        source="unionpay", ref=None):
    return svc.execute(
        "receive_payment", key,
        {"order_id": order, "company_id": company, "payment_reference": pref,
         "amount_cents": amount, "period_id": period, "source": source,
         "channel_ref": ref or f"U-{key}"}, OP,
    )


class BudgetAndFreezeTest(unittest.TestCase):
    def test_freeze_on_redemption_and_identity(self):
        svc = base_world()
        redeem(svc, "r1", "O1", gross=5000)
        view = svc.pool_view("Y2026")
        self.assertTrue(view["identity_total_check"])
        self.assertEqual(view["frozen_cents"], 2000)  # 券面值
        self.assertEqual(view["available_cents"], to_cents("10000") - 2000)

    def test_shared_year_pool_exhausted_across_activities(self):
        svc = base_world()
        # 夜游 50% 补贴、单笔封顶 50 元；连续大额核销耗尽共享池
        for i in range(200):
            redeem(svc, f"rn{i}", f"ON{i}", company="C2", batch="B-NIGHT",
                   code=f"NCODE-{i}", gross=10000)
        view = svc.pool_view("Y2026")
        self.assertLess(view["available_cents"], 5000)
        with self.assertRaises(BudgetExhaustedError):
            redeem(svc, "rnX", "ONX", company="C2", batch="B-NIGHT",
                   code="NCODE-X", gross=10000)

    def test_per_order_cap_and_ratio_rule(self):
        svc = base_world()
        redeem(svc, "r1", "O1", batch="B-POP", gross=100)  # 券面值2000但只消费1元
        self.assertEqual(svc.state.orders["O1"]["subsidy_cents"], 100)
        redeem(svc, "r2", "O2", batch="B-NIGHT", code="C2X",
               gross=10000, company="C2")  # 50% = 5000 命中封顶 5000
        self.assertEqual(svc.state.orders["O2"]["subsidy_cents"], 5000)

    def test_unmatched_payment_stays_out_of_settlement(self):
        svc = base_world()
        redeem(svc, "r1", "O1")  # 无支付
        st = svc.execute("settle_window", "s1", {"period_id": "P1"}, FIN)
        excluded = st.events[0].payload["excluded"]
        self.assertTrue(any(e["order_id"] == "O1" for e in excluded))
        # 额度仍在冻结，没有被结算
        self.assertEqual(svc.pool_view("Y2026")["settled_cents"], 0)
        self.assertEqual(svc.pool_view("Y2026")["frozen_cents"], 2000)


class OrderingTest(unittest.TestCase):
    def test_payment_before_redemption(self):
        svc = base_world()
        pay(svc, "u1", "O1", "UP1")
        redeem(svc, "r1", "O1", payment_ref="UP1")
        self.assertEqual(svc.state.orders["O1"]["state"], "frozen")

    def test_refund_before_order_is_parked_then_released(self):
        svc = base_world()
        # 退款回执先到（乱序）
        svc.execute(
            "receive_refund", "f0",
            {"order_id": "O1", "refund_id": "RF0", "amount_cents": 5000,
             "source": "brand", "channel_ref": "F0"}, OP,
        )
        redeem(svc, "r1", "O1")
        pay(svc, "u1", "O1", "UP1")
        order = svc.state.orders["O1"]
        self.assertEqual(order["state"], "refunded")  # 挂起退款在订单齐备后释放
        self.assertEqual(svc.pool_view("Y2026")["frozen_cents"], 0)

    def test_batch_ingest_partial_failure(self):
        svc = base_world()
        receipts = svc.ingest([
            {"type": "receive_redemption", "key": "g1",
             "payload": {"order_id": "O1", "company_id": "C1", "batch_id": "B-POP",
                         "code": "G1", "gross_cents": 5000, "period_id": "P1",
                         "source": "brand", "channel_ref": "RG1"}},
            {"type": "receive_redemption", "key": "g2",
             "payload": {"order_id": "O2", "company_id": "C1", "batch_id": "NO-SUCH",
                         "code": "G2", "gross_cents": 5000, "period_id": "P1",
                         "source": "brand", "channel_ref": "RG2"}},
        ], OP)
        self.assertFalse(hasattr(receipts[0], "error_type"))
        # 未知批次不直接丢弃：进隔离区等待人工核对
        self.assertTrue(receipts[1].quarantined)
        self.assertEqual(svc.quarantine_list()[0]["reason"], "bad_reference")
        self.assertEqual(len(svc.quarantine_list()), 1)


class RetransmitTest(unittest.TestCase):
    def _payload(self, order="O1", gross=5000):
        return {"order_id": order, "company_id": "C1", "batch_id": "B-POP",
                "code": f"CODE-{order}", "gross_cents": gross, "period_id": "P1",
                "source": "brand", "channel_ref": "SAME-REF"}

    def test_identical_retransmit_is_idempotent(self):
        svc = base_world()
        r1 = svc.execute("receive_redemption", "k1", self._payload(), OP)
        r2 = svc.execute("receive_redemption", "k2", self._payload(), OP)
        self.assertEqual(len(r1.events), 2)
        self.assertTrue(r2.duplicated)
        self.assertEqual(svc.pool_view("Y2026")["frozen_cents"], 2000)

    def test_same_amount_different_content_is_quarantined(self):
        svc = base_world()
        svc.execute("receive_redemption", "k1", self._payload(order="O1"), OP)
        # 渠道重传：流水号相同、金额相同但订单号/券码不同
        bad = self._payload(order="O9")
        bad["code"] = "CODE-O9"
        r2 = svc.execute("receive_redemption", "k2", bad, OP)
        self.assertTrue(r2.quarantined)
        self.assertEqual(svc.quarantine_list()[0]["reason"], "retransmit_diff")
        # 被隔离的记录没有冻结任何额度
        self.assertEqual(svc.pool_view("Y2026")["frozen_cents"], 2000)

    def test_same_key_different_payload_rejected(self):
        svc = base_world()
        svc.execute("receive_redemption", "k1", self._payload(gross=5000), OP)
        with self.assertRaises(DuplicateCommandError):
            svc.execute("receive_redemption", "k1", self._payload(gross=6000), OP)

    def test_fingerprint_ignores_memo_fields(self):
        p1 = {"a": 1, "note": "x", "key": "k"}
        p2 = {"a": 1, "note": "y", "key": "k"}
        self.assertEqual(fingerprint(p1), fingerprint(p2))


class DoubleSubsidyTest(unittest.TestCase):
    def test_same_unionpay_payment_cannot_get_two_subsidies(self):
        svc = base_world()
        redeem(svc, "r1", "O1", payment_ref="UP-SHARED")
        # 另一张券、另一个活动试图引用同一笔银联支付
        with self.assertRaises(RuleConflictError):
            redeem(svc, "r2", "O2", company="C2", batch="B-NIGHT",
                   code="CODE-O2", payment_ref="UP-SHARED")

    def test_coupon_code_single_use(self):
        svc = base_world()
        redeem(svc, "r1", "O1", code="TWICE")
        with self.assertRaises(RuleConflictError):
            redeem(svc, "r2", "O2", code="TWICE")


class SnapshotTest(unittest.TestCase):
    def _settled_and_closed(self, svc):
        redeem(svc, "r1", "O1", gross=5000)
        pay(svc, "u1", "O1", "UP1")
        svc.execute("settle_window", "s1", {"period_id": "P1"}, FIN)
        svc.execute("close_books", "x1", {"period_id": "P1"}, FIN)

    def test_closed_snapshot_is_immutable(self):
        svc = base_world()
        self._settled_and_closed(svc)
        snap_before = svc.snapshot("SNAP-P1")
        total_before = snap_before["totals_company"]["C1"]["subsidy_cents"]
        # 封账后商户补报同窗口核销：进隔离，快照不变
        r = redeem(svc, "rLate", "OLATE", code="LATE", gross=9000)
        self.assertTrue(r.quarantined)
        snap_after = svc.snapshot("SNAP-P1")
        self.assertEqual(
            snap_after["totals_company"]["C1"]["subsidy_cents"], total_before
        )
        self.assertEqual(snap_after["head_seq"], snap_before["head_seq"])

    def test_refund_after_close_creates_adjustment_never_rewrites(self):
        svc = base_world()
        self._settled_and_closed(svc)
        snap_subsidy = svc.snapshot("SNAP-P1")["entries"][0]["subsidy_cents"]
        # 跨店退款：退款由 C3 发起，但返还责任在领补贴的 C1
        r = svc.execute(
            "receive_refund", "f1",
            {"order_id": "O1", "refund_id": "RF1", "amount_cents": 5000,
             "company_id": "C3", "source": "brand", "channel_ref": "F1"}, OP,
        )
        refund_event = r.events[0].payload
        self.assertTrue(refund_event["cross_company"])
        self.assertEqual(refund_event["responsible_company_id"], "C1")
        self.assertEqual(refund_event["refunding_company_id"], "C3")
        # 快照原封不动
        self.assertEqual(
            svc.snapshot("SNAP-P1")["entries"][0]["subsidy_cents"], snap_subsidy
        )
        # 挂下期追减调整单，钱已回池
        pending = svc.adjustments("pending")
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["amount_cents"], snap_subsidy)
        self.assertTrue(svc.reconcile()["ok"])

    def test_refund_before_settle_releases(self):
        svc = base_world()
        redeem(svc, "r1", "O1")
        pay(svc, "u1", "O1", "UP1")
        svc.execute(
            "receive_refund", "f1",
            {"order_id": "O1", "refund_id": "RF1", "amount_cents": 5000,
             "source": "brand", "channel_ref": "F1"}, OP,
        )
        svc.execute("settle_window", "s1", {"period_id": "P1"}, FIN)
        doc = svc.state.settlements["ST-P1"]
        self.assertEqual(doc["entries"], [])
        self.assertEqual(svc.pool_view("Y2026")["frozen_cents"], 0)
        self.assertEqual(
            svc.pool_view("Y2026")["available_cents"], to_cents("10000")
        )

    def test_refund_after_settle_before_close_corrects_window(self):
        svc = base_world()
        redeem(svc, "r1", "O1")
        pay(svc, "u1", "O1", "UP1")
        svc.execute("settle_window", "s1", {"period_id": "P1"}, FIN)
        self.assertEqual(
            svc.state.settlements["ST-P1"]["totals_company"]["C1"]["orders"], 1
        )
        svc.execute(
            "receive_refund", "f1",
            {"order_id": "O1", "refund_id": "RF1", "amount_cents": 5000,
             "source": "brand", "channel_ref": "F1"}, OP,
        )
        svc.execute("close_books", "x1", {"period_id": "P1"}, FIN)
        snap = svc.snapshot("SNAP-P1")
        self.assertEqual(snap["entries"], [])  # 封账前已更正
        self.assertEqual(len(snap["clawbacks"]), 1)
        self.assertTrue(svc.reconcile()["ok"])

    def test_adjustment_applied_in_next_window(self):
        svc = base_world()
        redeem(svc, "r1", "O1", gross=5000)
        pay(svc, "u1", "O1", "UP1")
        svc.execute("settle_window", "s1", {"period_id": "P1"}, FIN)
        svc.execute("close_books", "x1", {"period_id": "P1"}, FIN)
        svc.execute(
            "receive_refund", "f1",
            {"order_id": "O1", "refund_id": "RF1", "amount_cents": 5000,
             "source": "brand", "channel_ref": "F1"}, OP,
        )
        self.assertEqual(len(svc.adjustments("pending")), 1)
        svc.execute("open_window", "w2", {"period_id": "P2", "activity_ids": ["POPUP", "NIGHT"]}, OP)
        redeem(svc, "r2", "O2", period="P2")
        pay(svc, "u2", "O2", "UP2", period="P2")
        st = svc.execute("settle_window", "s2", {"period_id": "P2"}, FIN)
        self.assertIn("ADJ-RF1", st.events[0].payload["applied_adjustments"])
        self.assertEqual(svc.adjustments("pending"), [])


class DisputeTest(unittest.TestCase):
    def test_only_auditor_can_unlock(self):
        svc = base_world()
        redeem(svc, "r1", "O1")
        pay(svc, "u1", "O1", "UP1")
        svc.execute("raise_dispute", "d1",
                    {"dispute_id": "D1", "order_id": "O1", "reason": "跨店归属存疑"}, OP)
        for actor in (OP, FIN, ORG):
            with self.assertRaises(AuthorizationError):
                svc.execute("resolve_dispute", "dx",
                            {"dispute_id": "D1", "decision": "release"}, actor)
        svc.execute("resolve_dispute", "d2",
                    {"dispute_id": "D1", "decision": "release"}, AUD)
        self.assertEqual(svc.state.orders["O1"]["state"], "released")

    def test_disputed_order_excluded_from_settlement(self):
        svc = base_world()
        redeem(svc, "r1", "O1")
        pay(svc, "u1", "O1", "UP1")
        svc.execute("raise_dispute", "d1",
                    {"dispute_id": "D1", "order_id": "O1", "reason": "存疑"}, OP)
        st = svc.execute("settle_window", "s1", {"period_id": "P1"}, FIN)
        self.assertEqual(st.events[0].payload["entries"], [])
        # 争议期间冻结不动
        self.assertEqual(svc.pool_view("Y2026")["frozen_cents"], 2000)
        # 争议未解时退款被拒绝
        with self.assertRaises(SettlementError):
            svc.execute("receive_refund", "f1",
                        {"order_id": "O1", "refund_id": "RF1", "amount_cents": 5000,
                         "source": "brand", "channel_ref": "F1"}, OP)
        svc.execute("resolve_dispute", "d2",
                    {"dispute_id": "D1", "decision": "uphold"}, AUD)
        # 维持原判：订单回到可结算状态，下一窗口分账
        svc.execute("open_window", "w2", {"period_id": "P2", "activity_ids": ["POPUP"]}, OP)
        st2 = svc.execute("settle_window", "s2", {"period_id": "P2"}, FIN)
        self.assertTrue(any(
            e["order_id"] == "O1" for e in st2.events[0].payload["entries"]
        ))

    def test_resolve_twice_rejected(self):
        svc = base_world()
        redeem(svc, "r1", "O1")
        svc.execute("raise_dispute", "d1",
                    {"dispute_id": "D1", "order_id": "O1", "reason": "x"}, OP)
        svc.execute("resolve_dispute", "d2",
                    {"dispute_id": "D1", "decision": "release"}, AUD)
        with self.assertRaises(DisputeStateError):
            svc.execute("resolve_dispute", "d3",
                        {"dispute_id": "D1", "decision": "release"}, AUD)

    def test_reassign_after_close(self):
        svc = base_world()
        redeem(svc, "r1", "O1")
        pay(svc, "u1", "O1", "UP1")
        svc.execute("settle_window", "s1", {"period_id": "P1"}, FIN)
        svc.execute("close_books", "x1", {"period_id": "P1"}, FIN)
        svc.execute("raise_dispute", "d1",
                    {"dispute_id": "D1", "order_id": "O1", "reason": "归属争议"}, OP)
        svc.execute("resolve_dispute", "d2",
                    {"dispute_id": "D1", "decision": "reassign",
                     "new_company_id": "C3", "note": "实际经营方为快闪品牌"}, AUD)
        adj = [a for a in svc.adjustments("pending")
               if a["type"] == "dispute_reassign"][0]
        self.assertEqual(adj["company_id"], "C1")
        self.assertEqual(adj["new_company_id"], "C3")
        self.assertEqual(adj["snapshot_id"], "SNAP-P1")
        # reassign 不重复扣款（钱在原企业与新企业之间改派，由下期调整单解释）
        self.assertTrue(svc.reconcile()["ok"])


class RbacTest(unittest.TestCase):
    def test_finance_only_handles_money(self):
        svc = base_world()
        with self.assertRaises(AuthorizationError):
            svc.execute("deposit_budget", "x",
                        {"pool_id": "P", "amount_cents": 100}, OP)
        with self.assertRaises(AuthorizationError):
            svc.execute("define_activity", "x",
                        {"activity_id": "A", "name": "n", "pool_id": "Y2026"}, FIN)
        with self.assertRaises(AuthorizationError):
            svc.execute("receive_redemption", "x", {}, FIN)

    def test_unknown_role_rejected(self):
        with self.assertRaises(ValueError):
            Principal("x", "root")


class PersistenceTest(unittest.TestCase):
    def test_restart_replay_restores_state(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "journal.jsonl"
            svc = base_world(path)
            redeem(svc, "r1", "O1")
            pay(svc, "u1", "O1", "UP1")
            svc.execute("settle_window", "s1", {"period_id": "P1"}, FIN)
            svc.execute("close_books", "x1", {"period_id": "P1"}, FIN)
            head = svc.head_digest

            svc2 = SettlementService(path)  # 重启
            self.assertEqual(svc2.head_digest, head)
            self.assertTrue(svc2.reconcile()["ok"])
            self.assertEqual(svc2.state.orders["O1"]["state"], "settled")
            self.assertIn("SNAP-P1", svc2.state.snapshots)
            # 重放后幂等仍生效
            r = svc2.execute(
                "receive_payment", "u1",
                {"order_id": "O1", "company_id": "C1", "payment_reference": "UP1",
                 "amount_cents": 5000, "period_id": "P1", "source": "unionpay",
                 "channel_ref": "U-u1"}, OP,
            )
            self.assertTrue(r.duplicated)

    def test_tamper_detected(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "journal.jsonl"
            svc = base_world(path)
            redeem(svc, "r1", "O1")
            lines = path.read_text(encoding="utf-8").splitlines()
            # 篡改最后一行金额
            import json
            data = json.loads(lines[-1])
            data["payload"]["amount_cents"] = 999999
            lines[-1] = json.dumps(data, ensure_ascii=False)
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                Journal(path)


class TraceabilityTest(unittest.TestCase):
    def test_drill_from_summary_to_redemption(self):
        svc = base_world()
        redeem(svc, "r1", "O1", gross=5000)
        pay(svc, "u1", "O1", "UP1")
        svc.execute("settle_window", "s1", {"period_id": "P1"}, FIN)
        svc.execute("close_books", "x1", {"period_id": "P1"}, FIN)
        traced = svc.trace_settlement("ST-P1")
        # 汇总 -> 企业合计 -> 单笔分录 -> 原始核销/支付事件序号
        self.assertEqual(traced["settlement"]["totals_company"]["C1"]["orders"], 1)
        item = traced["entries_with_provenance"][0]
        self.assertTrue(item["redemption_event_seq"] > 0)
        self.assertTrue(item["payment_event_seq"] > 0)
        self.assertIn("规则版本", item["entry"]["explanation"])
        # 快照里的汇总数与分录逐项加总相等
        snap = traced["snapshot"]
        self.assertEqual(
            snap["totals_company"]["C1"]["subsidy_cents"],
            sum(e["subsidy_cents"] for e in snap["entries"]
                if e["company_id"] == "C1"),
        )

    def test_order_timeline_covers_every_change(self):
        svc = base_world()
        redeem(svc, "r1", "O1")
        pay(svc, "u1", "O1", "UP1")
        svc.execute("settle_window", "s1", {"period_id": "P1"}, FIN)
        svc.execute("close_books", "x1", {"period_id": "P1"}, FIN)
        svc.execute(
            "receive_refund", "f1",
            {"order_id": "O1", "refund_id": "RF1", "amount_cents": 5000,
             "source": "brand", "channel_ref": "F1"}, OP,
        )
        trace = svc.trace_order("O1")
        steps = {s["step"].split(" ")[0] for s in trace["timeline"]}
        self.assertIn("redemption", steps)
        self.assertIn("payment", steps)
        self.assertIn("窗口分账", [s["step"] for s in trace["timeline"]])
        self.assertTrue(any("退款" in s["step"] for s in trace["timeline"]))
        self.assertEqual(trace["snapshot_id"], "SNAP-P1")
        self.assertEqual(len(trace["adjustments"]), 1)


class SettlementContentTest(unittest.TestCase):
    def test_split_explanation_identifies_company_and_rule(self):
        svc = base_world()
        redeem(svc, "r1", "O1", company="C2", batch="B-NIGHT",
               code="N1", gross=10000)
        pay(svc, "u1", "O1", "UP1", company="C2", amount=10000)
        svc.execute("settle_window", "s1", {"period_id": "P1"}, FIN)
        item = svc.explain_entry("ST-P1", "O1")
        self.assertEqual(item["entry"]["company_id"], "C2")
        self.assertEqual(item["entry"]["subsidy_cents"], 5000)
        self.assertIn("NIGHT", item["entry"]["explanation"])


if __name__ == "__main__":
    unittest.main()
