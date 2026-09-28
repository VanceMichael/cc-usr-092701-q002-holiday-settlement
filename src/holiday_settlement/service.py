"""结算协同服务核心。

一个事件源（event-sourced）的领域服务：所有命令先判定、再把决定写入仅追加
事件日志，状态由事件重放得到。关键业务约束：

1. 额度先冻结、结算窗口再转已结算；退款按比例冲回冻结或已结算金额。
2. 年度预算池可被多个活动共享，但同一支付单（消费去重键）全活动只能享受
   一次补贴；"同记录号、不同内容指纹"的重传进隔离区，不覆盖原记录。
3. 封账快照一次性落盘、永不改写；封账后补报只能形成可追溯的后期调整。
4. 跨店退款等责任不清的单据进入争议，补贴款项挂起，只有审计角色可解锁。
5. 支付/核销/退款乱序到达时先挂起，依赖齐备后自动重新处理；重启后通过
   重放事件得到完全一致的额度、退款与已结算金额。

角色分工：财务只做金额动作，主办方维护规则，审计解锁争议。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from .model import (
    AnnualBudget, BatchState, BatchSpec, BudgetSpec, Campaign,
    CampaignSpec, PaymentData, RedemptionData, RefundData, Role, User,
    content_fingerprint, money, rate,
)
from .store import EventStore, to_jsonable

CENT = Decimal("0.01")


def _share(value: Decimal) -> Decimal:
    return value.quantize(CENT)


class PermissionDenied(PermissionError):
    pass


class RuleViolation(ValueError):
    """明确的硬性规则违反（如重复补贴、重复封账），不可挂起、不可改写。"""


# ---------------------------------------------------------------- 运行态状态

@dataclass
class OrderState:
    redemption_id: str
    record_id: str
    campaign_id: str
    merchant_id: str
    payment_id: str
    coupon_code: str
    amount: Decimal
    subsidy: Decimal
    fingerprint: str
    accepted_seq: int
    manual: bool = False
    status: str = "accepted"          # accepted | settled | late_pending | late_settled
    frozen: Decimal = Decimal("0.00")
    settled: Decimal = Decimal("0.00")
    recovered: Decimal = Decimal("0.00")
    recovered_post: Decimal = Decimal("0.00")   # 封账后（含后期补付后）累计应返还
    refunded_amount: Decimal = Decimal("0.00")
    sealed_in: str | None = None
    dispute_id: str | None = None


@dataclass
class Dispute:
    dispute_id: str
    kind: str                        # cross_store_refund | quarantined_record
    ref: str
    campaign_id: str | None
    reason: str
    opened_seq: int
    status: str = "open"
    resolution: dict[str, Any] | None = None
    resolved_seq: int | None = None


@dataclass
class PendingItem:
    kind: str                        # redemption | refund
    record_id: str
    data: Any
    wait: str                        # await_payment | await_quota | await_order
    fingerprint: str


class SettlementService:
    """内存态由事件重放构建；所有外部入口都是命令或查询。"""

    def __init__(self, store: EventStore, today: date | None = None):
        self.store = store
        self.today = today or date.today()
        self.budgets: dict[str, AnnualBudget] = {}
        self.campaigns: dict[str, Campaign] = {}
        self.batches: dict[str, BatchState] = {}
        self.payments: dict[str, PaymentData] = {}
        self.orders: dict[str, OrderState] = {}
        self.by_payment: dict[str, str] = {}        # payment_id -> redemption_id
        self.spent_keys: dict[str, tuple[str, str]] = {}  # payment_id -> (活动, 核销单)
        self.refunds: dict[str, dict[str, Any]] = {}      # refund_id -> 退款记录与结论
        self.records: dict[tuple[str, str], str] = {}    # (kind, record_id) -> 指纹
        self.quarantine: dict[str, dict[str, Any]] = {}
        self.rejected: dict[str, dict[str, Any]] = {}
        self.disputes: dict[str, Dispute] = {}
        self.adjustments: list[dict[str, Any]] = []
        self.pending: dict[tuple[str, str], PendingItem] = {}
        self._dispute_seq = 0
        self._replay(store.read_events())

    # ================================================================ 装载

    @classmethod
    def load(cls, directory: str | Path, today: date | None = None) -> "SettlementService":
        """从事件日志目录重启服务，完整重放并重建挂起队列。"""
        return cls(EventStore(directory), today=today)

    def _replay(self, events: list) -> None:
        for event in events:
            self._apply(event, persist=False)
        self._finalize_replay()

    # ------------------------------------------------------------ 配置命令

    def register_budget(self, user: User, spec: BudgetSpec) -> None:
        self._require(user, Role.ORGANIZER, "登记年度预算池")
        if spec.budget_id in self.budgets:
            raise RuleViolation(f"预算池 {spec.budget_id} 已存在")
        if spec.total < 0:
            raise RuleViolation("预算总额不能为负")
        self._append("budget_registered", user.name, {
            "budget_id": spec.budget_id, "total": money(spec.total),
        })

    def register_campaign(self, user: User, spec: CampaignSpec) -> None:
        self._require(user, Role.ORGANIZER, "登记活动规则")
        if spec.campaign_id in self.campaigns:
            raise RuleViolation(f"活动 {spec.campaign_id} 已存在")
        if (spec.budget_id is None) == (spec.standalone_budget is None):
            raise RuleViolation("活动必须且只能选择共享预算池或独立额度之一")
        if spec.budget_id is not None and spec.budget_id not in self.budgets:
            raise RuleViolation(f"预算池 {spec.budget_id} 尚未登记")
        total = (
            self.budgets[spec.budget_id].total if spec.budget_id is not None
            else money(spec.standalone_budget)
        )
        self._append("campaign_registered", user.name, {
            "campaign_id": spec.campaign_id,
            "name": spec.name,
            "budget_id": spec.budget_id,
            "budget_kind": "shared" if spec.budget_id else "standalone",
            "budget_total": total,
            "discount_rate": rate(spec.discount_rate),
            "max_subsidy_per_order": (
                money(spec.max_subsidy_per_order)
                if spec.max_subsidy_per_order is not None else None
            ),
            "settlement_day": spec.settlement_day,
            "order_dedup_field": spec.order_dedup_field,
            "merchants": sorted(spec.merchants),
        })

    def register_batch(self, user: User, spec: BatchSpec) -> None:
        self._require(user, Role.ORGANIZER, "登记券批次")
        if spec.batch_id in self.batches:
            raise RuleViolation(f"券批次 {spec.batch_id} 已存在")
        if spec.campaign_id not in self.campaigns:
            raise RuleViolation(f"活动 {spec.campaign_id} 尚未登记")
        if not spec.codes:
            raise RuleViolation("券批次至少包含一个券码")
        self._append("batch_registered", user.name, {
            "batch_id": spec.batch_id,
            "campaign_id": spec.campaign_id,
            "codes": sorted(spec.codes),
            "subsidy_quota": money(spec.subsidy_quota),
        })

    # ------------------------------------------------------------ 数据接入

    def ingest_payment(self, user: User, data: PaymentData) -> str:
        """银联/渠道支付对账记录（分批送达）。"""
        status = self._dedupe_gate(
            ("payment", data.record_id),
            content_fingerprint(self._payment_payload(data)),
        )
        if status == "ignored_duplicate":
            return status
        if status == "fingerprint_conflict":
            return self._quarantine_retransmit(user.name, "payment", data.record_id,
                                               self._payment_payload(data))
        existing = self.payments.get(data.payment_id)
        if existing is not None and (
            existing.amount != data.amount or existing.merchant_id != data.merchant_id
        ):
            return self._quarantine(
                user.name, "payment", data.record_id, "payment_conflict",
                content_fingerprint(self._payment_payload(data)),
                self._payment_payload(data),
                detail={"existing_record": existing.record_id}, open_dispute=False,
            )
        fp = content_fingerprint(self._payment_payload(data))
        self._append("payment_received", user.name, {
            **self._payment_payload(data), "fingerprint": fp,
        })
        self._pump(user.name)
        return "accepted"

    def ingest_redemption(self, user: User, data: RedemptionData) -> str:
        fp = content_fingerprint(self._redemption_payload(data))
        status = self._dedupe_gate(("redemption", data.record_id), fp)
        if status == "ignored_duplicate":
            return status
        if status == "fingerprint_conflict":
            return self._quarantine_retransmit(user.name, "redemption", data.record_id,
                                               self._redemption_payload(data), fp)
        self._append("redemption_received", user.name, {
            **self._redemption_payload(data), "fingerprint": fp,
        })
        self._pump(user.name)
        return self._result_for_record(("redemption", data.record_id))

    def ingest_refund(self, user: User, data: RefundData) -> str:
        fp = content_fingerprint(self._refund_payload(data))
        status = self._dedupe_gate(("refund", data.record_id), fp)
        if status == "ignored_duplicate":
            return status
        if status == "fingerprint_conflict":
            return self._quarantine_retransmit(user.name, "refund", data.record_id,
                                               self._refund_payload(data), fp)
        self._append("refund_received", user.name, {
            **self._refund_payload(data), "fingerprint": fp,
        })
        self._pump(user.name)
        return self._result_for_record(("refund", data.record_id))

    # -------------------------------------------------------- 结算窗口/财务

    def seal_window(self, user: User, campaign_id: str) -> dict[str, Any]:
        """封账：把窗口内核销单的冻结额度转为已结算，生成不可变快照。"""
        self._require(user, Role.FINANCE, "封账并生成分账快照")
        campaign = self._campaign(campaign_id)
        if campaign.sealed:
            raise RuleViolation(f"活动 {campaign_id} 已封账，快照不可改写")
        if self.today < campaign.spec.settlement_day:
            raise RuleViolation(f"未到结算日 {campaign.spec.settlement_day}，不能提前封账")
        self._pump(user.name)  # 尽量消化挂起；仍挂起/挂争议的不进快照

        snapshot_id = f"snap-{campaign_id}-{campaign.spec.settlement_day.isoformat()}"
        lines: dict[str, dict[str, Any]] = {}
        held: list[str] = []
        settled_this = Decimal("0.00")
        for order in self.orders.values():
            if order.campaign_id != campaign_id or order.status != "accepted":
                continue
            if order.dispute_id:
                held.append(order.redemption_id)
                continue
            line = lines.setdefault(order.merchant_id, {
                "merchant_id": order.merchant_id, "order_ids": [],
                "gross": Decimal("0.00"), "subsidy": Decimal("0.00"),
            })
            line["order_ids"].append(order.redemption_id)
            line["gross"] += order.amount - order.refunded_amount
            line["subsidy"] += order.frozen
            settled_this += order.frozen

        snapshot = {
            "campaign_id": campaign_id,
            "campaign_name": campaign.spec.name,
            "sealed_date": self.today,
            "settlement_day": campaign.spec.settlement_day,
            "rule": {
                "discount_rate": campaign.spec.discount_rate,
                "max_subsidy_per_order": campaign.spec.max_subsidy_per_order,
                "budget_kind": campaign.budget_kind,
                "budget_total": campaign.budget_total,
                "rule_explanation": self._rule_text(campaign),
            },
            "budget": {
                "frozen_before": campaign.frozen,
                "settled_before": campaign.settled,
                "settled_this_window": settled_this,
                "frozen_after": campaign.frozen - settled_this,
                "settled_after": campaign.settled + settled_this,
            },
            "merchants": [lines[k] for k in sorted(lines)],
            "held_disputes": sorted(held),
            "pending_records": sorted(
                r.record_id for r in self.pending.values()
                if r.kind == "redemption" and getattr(r.data, "campaign_id", None) == campaign_id
            ),
            "tail_event_hash": self.store._tail,
            "tail_event_seq": self.store._seq,
        }
        self.store.write_snapshot(snapshot_id, snapshot)
        self._append("window_sealed", user.name, {
            "campaign_id": campaign_id,
            "snapshot_id": snapshot_id,
            "frozen_settled": settled_this,
            "merchant_count": len(lines),
            "held_disputes": sorted(held),
        })
        return self.snapshot(campaign_id)

    def confirm_late_order(self, user: User, redemption_id: str, note: str = "") -> dict:
        """封账后补报核销经财务确认，形成后期调整，不动快照。"""
        self._require(user, Role.FINANCE, "确认封账后补报")
        order = self._order(redemption_id)
        campaign = self._campaign(order.campaign_id)
        if not campaign.sealed:
            raise RuleViolation("活动尚未封账，补报应在窗口内正常结算")
        if order.status != "late_pending":
            raise RuleViolation(f"核销单 {redemption_id} 状态为 {order.status}，无需补报确认")
        if order.dispute_id:
            raise RuleViolation("争议未解锁，不能确认补报")
        net = order.frozen  # 已扣除封账后退款冲回的净补贴
        self._append("late_order_confirmed", user.name, {
            "redemption_id": redemption_id,
            "campaign_id": order.campaign_id,
            "snapshot_id": campaign.sealed_snapshot_id,
            "subsidy_original": order.subsidy,
            "subsidy_paid": net,
            "note": note,
        })
        return self.get_order(redemption_id)

    # ------------------------------------------------------------ 争议/审计

    def resolve_dispute(self, user: User, dispute_id: str, resolution: dict[str, Any]) -> dict[str, Any]:
        """只有审计角色能解锁争议单；解锁决定后续金额走向。"""
        self._require(user, Role.AUDITOR, "解锁争议单")
        dispute = self.disputes.get(dispute_id)
        if dispute is None:
            raise RuleViolation(f"争议单 {dispute_id} 不存在")
        if dispute.status != "open":
            raise RuleViolation(f"争议单 {dispute_id} 已解锁")
        decision = resolution.get("decision")
        valid = {"apply_refund", "dismiss_refund", "accept_record", "reject_record"}
        if decision not in valid:
            raise RuleViolation(f"解锁决定必须是 {'/'.join(sorted(valid))}")
        self._append("dispute_resolved", user.name, {
            "dispute_id": dispute_id,
            "decision": decision,
            "resolution": resolution,
        })
        if dispute.kind == "cross_store_refund":
            record = self.refunds[dispute.ref]
            if decision == "apply_refund":
                self._append("refund_applied", user.name, {
                    "refund_id": record["refund_id"],
                    "record_id": record["record_id"],
                    "payment_id": record["payment_id"],
                    "refund_amount": record["amount"],
                    "responsible_merchant": resolution.get(
                        "responsible_merchant", record["subsidy_merchant"]
                    ),
                })
            # dismiss_refund：款项保留，封账后的冻结净额转由财务走补报确认
        elif dispute.kind == "quarantined_record" and decision == "accept_record":
            self._accept_quarantined(dispute.ref, user.name, resolution)
        self._pump(user.name)
        return self.get_dispute(dispute_id)

    # ================================================================ 查询

    def budget_view(self, budget_id: str) -> dict[str, Any]:
        pool = self.budgets[budget_id]
        members = [cid for cid, c in self.campaigns.items() if c.spec.budget_id == budget_id]
        return {
            "budget_id": budget_id,
            "total": pool.total,
            "frozen": pool.frozen,
            "settled": pool.settled,
            "late_settled": pool.late_settled,
            "recovered": pool.recovered,
            "available": pool.total - pool.frozen - pool.settled - pool.late_settled,
            "campaigns": members,
        }

    def campaign_view(self, campaign_id: str) -> dict[str, Any]:
        c = self._campaign(campaign_id)
        return {
            "campaign_id": campaign_id,
            "name": c.spec.name,
            "sealed": c.sealed,
            "snapshot_id": c.sealed_snapshot_id,
            "budget_total": c.budget_total,
            "frozen": c.frozen,
            "settled": c.settled,
            "late_settled": c.late_settled,
            "recovered": c.recovered,
            "available": c.available_budget,
        }

    def splits(self, campaign_id: str) -> dict[str, Any]:
        """可解释分账：封账快照行 + 封账后调整，逐行列出规则与构成订单。"""
        c = self._campaign(campaign_id)
        result: dict[str, Any] = {
            "campaign_id": campaign_id,
            "rule": {
                "discount_rate": c.spec.discount_rate,
                "max_subsidy_per_order": c.spec.max_subsidy_per_order,
                "rule_explanation": self._rule_text(c),
            },
            "window": [],
            "post_seal_adjustments": [
                a for a in self.adjustments if a["campaign_id"] == campaign_id
            ],
        }
        if c.sealed_snapshot_id:
            snap = self._decode_snapshot(c.sealed_snapshot_id)
            result["snapshot_id"] = c.sealed_snapshot_id
            result["window"] = snap["merchants"]
        return result

    def get_order(self, redemption_id: str) -> dict[str, Any]:
        o = self._order(redemption_id)
        return {
            "redemption_id": o.redemption_id,
            "record_id": o.record_id,
            "campaign_id": o.campaign_id,
            "merchant_id": o.merchant_id,
            "payment_id": o.payment_id,
            "coupon_code": o.coupon_code,
            "amount": o.amount,
            "subsidy": o.subsidy,
            "status": o.status,
            "manual": o.manual,
            "frozen": o.frozen,
            "settled": o.settled,
            "recovered": o.recovered,
            "recovered_post_seal": o.recovered_post,
            "refunded_amount": o.refunded_amount,
            "sealed_in": o.sealed_in,
            "dispute_id": o.dispute_id,
            "fingerprint": o.fingerprint,
            "accepted_seq": o.accepted_seq,
            "explanation": self._explain_order(o),
        }

    def get_dispute(self, dispute_id: str) -> dict[str, Any]:
        d = self.disputes[dispute_id]
        return {
            "dispute_id": d.dispute_id, "kind": d.kind, "ref": d.ref,
            "campaign_id": d.campaign_id, "reason": d.reason,
            "status": d.status, "resolution": d.resolution,
            "opened_seq": d.opened_seq, "resolved_seq": d.resolved_seq,
        }

    def list_open_disputes(self) -> list[dict[str, Any]]:
        return [self.get_dispute(d.dispute_id)
                for d in sorted(self.disputes.values(), key=lambda x: x.opened_seq)
                if d.status == "open"]

    def list_pending(self) -> list[dict[str, Any]]:
        return [
            {"kind": p.kind, "record_id": p.record_id, "wait": p.wait,
             "campaign_id": getattr(p.data, "campaign_id", None)}
            for p in sorted(self.pending.values(), key=lambda x: x.record_id)
        ]

    def list_quarantine(self) -> list[dict[str, Any]]:
        return [self.quarantine[k] for k in sorted(self.quarantine)]

    def list_rejected(self) -> list[dict[str, Any]]:
        return [self.rejected[k] for k in sorted(self.rejected)]

    def snapshot(self, campaign_id: str) -> dict[str, Any]:
        c = self._campaign(campaign_id)
        if not c.sealed_snapshot_id:
            raise RuleViolation(f"活动 {campaign_id} 尚未封账")
        return self._decode_snapshot(c.sealed_snapshot_id)

    def trace(self, campaign_id: str, merchant_id: str | None = None) -> dict[str, Any]:
        """从一笔汇总数字追到原始核销、支付、退款与每次调整。"""
        c = self._campaign(campaign_id)
        result: dict[str, Any] = {
            "campaign_id": campaign_id,
            "campaign_view": self.campaign_view(campaign_id),
            "snapshot": None,
            "merchants": [],
        }
        splits = self.splits(campaign_id)
        if c.sealed_snapshot_id:
            raw = self.store.read_snapshot(c.sealed_snapshot_id)
            result["snapshot"] = {
                "snapshot_id": c.sealed_snapshot_id,
                "fingerprint": raw["snapshot_fingerprint"],
            }
        for line in splits["window"]:
            if merchant_id and line["merchant_id"] != merchant_id:
                continue
            result["merchants"].append(self._trace_window_line(line))
        for adj in splits["post_seal_adjustments"]:
            if merchant_id and adj.get("merchant_id") != merchant_id:
                continue
            result["merchants"].append({
                "source": "post_seal_adjustment",
                **adj,
                "evidence": self._evidence(adj.get("redemption_id")),
            })
        return result

    def reconcile(self) -> dict[str, Any]:
        """额度勾稽：预算池/活动/批次/订单四层金额必须相互吻合。"""
        checks: list[dict[str, Any]] = []

        def ok(name: str, condition: bool, detail: Any = None) -> None:
            checks.append({"check": name, "passed": bool(condition), "detail": detail})

        orders = list(self.orders.values())
        for cid, c in self.campaigns.items():
            mine = [o for o in orders if o.campaign_id == cid]
            sum_frozen = sum((o.frozen for o in mine), Decimal("0.00"))
            sum_settled = sum(
                (o.settled for o in mine if o.status == "settled"), Decimal("0.00"))
            sum_late = sum(
                (o.settled for o in mine if o.status == "late_settled"), Decimal("0.00"))
            sum_post = sum((o.recovered_post for o in mine), Decimal("0.00"))
            ok(f"{cid}:活动冻结=订单冻结", c.frozen == sum_frozen,
               {"campaign": str(c.frozen), "orders": str(sum_frozen)})
            ok(f"{cid}:活动已结算=窗口订单已结算", c.settled == sum_settled,
               {"campaign": str(c.settled), "orders": str(sum_settled)})
            ok(f"{cid}:活动后期补付=补报订单已结算", c.late_settled == sum_late,
               {"campaign": str(c.late_settled), "orders": str(sum_late)})
            ok(f"{cid}:活动应返还=订单封账后返还", c.recovered == sum_post,
               {"campaign": str(c.recovered), "orders": str(sum_post)})
            used = c.frozen + c.settled + c.late_settled
            ok(f"{cid}:不超预算", used <= c.budget_total + CENT,
               {"used": str(used), "total": str(c.budget_total)})
            for o in mine:
                # 补贴恒等式：冻结中 + 已拨付 + 封账前释放 = 应补贴；
                # 封账后返还不回冲快照已结算，而是对商户的应收回款（不超过已拨付）。
                balanced = o.frozen + o.settled + o.recovered == o.subsidy
                receivable_ok = Decimal("0.00") <= o.recovered_post <= o.settled + CENT
                ok(f"{cid}/{o.redemption_id}:冻结+已结算+封账前冲回=补贴", balanced,
                   {"subsidy": str(o.subsidy), "frozen": str(o.frozen),
                    "settled": str(o.settled), "pre_seal_recovered": str(o.recovered)})
                ok(f"{cid}/{o.redemption_id}:封账后返还不超过已拨付", receivable_ok,
                   {"settled": str(o.settled),
                    "post_seal_recovered": str(o.recovered_post)})

        for bid, pool in self.budgets.items():
            members = [c for c in self.campaigns.values() if c.spec.budget_id == bid]
            sf = sum((c.frozen for c in members), Decimal("0.00"))
            ss = sum((c.settled for c in members), Decimal("0.00"))
            sl = sum((c.late_settled for c in members), Decimal("0.00"))
            ok(f"{bid}:共享池冻结=各活动之和", pool.frozen == sf,
               {"pool": str(pool.frozen), "campaigns": str(sf)})
            ok(f"{bid}:共享池已结算=各活动之和", pool.settled == ss,
               {"pool": str(pool.settled), "campaigns": str(ss)})
            ok(f"{bid}:共享池后期=各活动之和", pool.late_settled == sl,
               {"pool": str(pool.late_settled), "campaigns": str(sl)})

        for bid_s, b in self.batches.items():
            used = b.frozen + b.settled + b.late_settled
            ok(f"{bid_s}:券批次不超额度", used <= b.spec.subsidy_quota + CENT,
               {"used": str(used), "quota": str(b.spec.subsidy_quota)})

        owners = list(self.spent_keys.values())
        ok("消费去重键全局唯一（一笔消费至多一次补贴）", len(owners) == len(set(owners)))
        all_ok = all(x["passed"] for x in checks)
        return {"passed": all_ok, "checks": checks,
                "event_chain_intact": self.store.verify_chain()}

    # ============================================================ 内部处理

    def _pump(self, actor: str) -> None:
        """反复尝试消化挂起队列，直到没有新进展。"""
        progressed = True
        while progressed:
            progressed = False
            for key in list(self.pending):
                item = self.pending[key]
                if item.kind == "redemption":
                    decision = self._evaluate_redemption(item.data)
                else:
                    decision = self._evaluate_refund(item.data)
                if decision["action"] == "wait":
                    item.wait = decision["wait"]
                    continue
                del self.pending[key]
                progressed = True
                self._dispatch(item, decision, actor)

    def _finalize_replay(self) -> None:
        """重放结束：恢复挂起队列。

        - 已有结论事件的占位挂起项清除；
        - 依赖仍未齐备的保留并标注等待原因；
        - 崩溃前未及写出结论、但现在已可定论的，重新派发（补写决定事件），
          保证重启不丢单。
        """
        actionable: list[PendingItem] = []
        for key in list(self.pending):
            kind, record_id = key
            item = self.pending[key]
            if kind == "redemption":
                concluded = (
                    self._quarantine_contains(record_id)
                    or record_id in self.rejected
                    or any(o.record_id == record_id for o in self.orders.values())
                )
                decision = None if concluded else self._evaluate_redemption(item.data)
            else:
                rec = next((r for r in self.refunds.values()
                            if r["record_id"] == record_id), None)
                concluded = (
                    self._quarantine_contains(record_id)
                    or (rec is not None and rec["status"] in ("applied", "disputed"))
                )
                decision = (
                    None if concluded or rec is None
                    else self._evaluate_refund(item.data)
                )
            if concluded or decision is None:
                self.pending.pop(key, None)
            elif decision["action"] == "wait":
                item.wait = decision["wait"]
            else:
                actionable.append(item)
        for item in actionable:
            self.pending.pop((item.kind, item.record_id), None)
            decision = (self._evaluate_redemption(item.data) if item.kind == "redemption"
                        else self._evaluate_refund(item.data))
            if decision["action"] != "wait":
                self._dispatch(item, decision, "SYSTEM-REPLAY")

    def _quarantine_contains(self, record_id: str) -> bool:
        if record_id in self.quarantine:
            return True
        return any(q.get("original_record_id") == record_id
                   for q in self.quarantine.values())

    def _evaluate_redemption(self, d: RedemptionData) -> dict[str, Any]:
        campaign = self.campaigns.get(d.campaign_id)
        if campaign is None:
            return {"action": "quarantine", "reason": "unknown_campaign"}
        if campaign.spec.merchants and d.merchant_id not in campaign.spec.merchants:
            return {"action": "quarantine", "reason": "merchant_not_in_campaign"}
        batch = self._batch_for_code(d.campaign_id, d.coupon_code)
        if batch is None:
            return {"action": "quarantine", "reason": "unknown_coupon"}
        payment = self.payments.get(d.payment_id)
        if payment is None:
            return {"action": "wait", "wait": "await_payment"}
        if payment.merchant_id != d.merchant_id:
            return {"action": "quarantine", "reason": "payment_merchant_mismatch",
                    "detail": {"payment_merchant": payment.merchant_id,
                               "redemption_merchant": d.merchant_id}}
        if payment.amount != d.amount:
            return {"action": "quarantine", "reason": "amount_mismatch",
                    "detail": {"payment_amount": str(payment.amount),
                               "redemption_amount": str(d.amount)}}
        if d.payment_id in self.spent_keys:
            owner_campaign, owner = self.spent_keys[d.payment_id]
            return {"action": "reject", "reason": "duplicate_subsidy",
                    "detail": {"owner_campaign": owner_campaign,
                               "owner_redemption": owner}}
        if d.coupon_code in batch.used_codes:
            return {"action": "reject", "reason": "coupon_already_redeemed",
                    "detail": {"batch_id": batch.spec.batch_id,
                               "coupon_code": d.coupon_code}}
        subsidy = self._calc_subsidy(campaign, d.amount)
        available = self._available_for(campaign)
        if subsidy > available + CENT or subsidy > batch.available + CENT:
            return {"action": "wait", "wait": "await_quota",
                    "detail": {"need": str(subsidy),
                               "fund_available": str(available),
                               "batch_available": str(batch.available)}}
        return {"action": "accept", "subsidy": subsidy, "batch_id": batch.spec.batch_id}

    def _evaluate_refund(self, d: RefundData) -> dict[str, Any]:
        order_id = self.by_payment.get(d.payment_id)
        if order_id is None:
            return {"action": "wait", "wait": "await_order"}
        order = self.orders[order_id]
        if d.amount > order.amount - order.refunded_amount + CENT:
            return {"action": "quarantine", "reason": "refund_exceeds_order",
                    "detail": {"refund_amount": str(d.amount),
                               "order_remaining": str(order.amount - order.refunded_amount)}}
        if d.merchant_id != order.merchant_id:
            return {"action": "dispute", "reason": "cross_store_refund",
                    "detail": {"refund_merchant": d.merchant_id,
                               "subsidy_merchant": order.merchant_id}}
        return {"action": "apply", "responsible_merchant": order.merchant_id}

    def _dispatch(self, item: PendingItem, decision: dict[str, Any], actor: str) -> None:
        d = item.data
        if item.kind == "redemption":
            if decision["action"] == "accept":
                self._append("redemption_accepted", actor, {
                    "redemption_id": d.redemption_id,
                    "record_id": d.record_id,
                    "campaign_id": d.campaign_id,
                    "merchant_id": d.merchant_id,
                    "payment_id": d.payment_id,
                    "coupon_code": d.coupon_code,
                    "amount": d.amount,
                    "subsidy": decision["subsidy"],
                    "batch_id": decision["batch_id"],
                    "fingerprint": item.fingerprint,
                    "manual": False,
                    "rule_path": self._rule_text(self.campaigns[d.campaign_id]),
                })
            elif decision["action"] == "quarantine":
                self._quarantine(actor, "redemption", d.record_id, decision["reason"],
                                 item.fingerprint, self._redemption_payload(d),
                                 decision.get("detail"))
            else:  # reject
                self._append("redemption_rejected", actor, {
                    "record_kind": "redemption", "record_id": d.record_id,
                    "reason": decision["reason"],
                    "fingerprint": item.fingerprint,
                    "payload": self._redemption_payload(d),
                    "detail": decision.get("detail", {}),
                })
        else:
            if decision["action"] == "apply":
                self._append("refund_applied", actor, {
                    "refund_id": d.refund_id, "record_id": d.record_id,
                    "payment_id": d.payment_id, "refund_amount": d.amount,
                    "responsible_merchant": decision["responsible_merchant"],
                })
            elif decision["action"] == "quarantine":
                self._quarantine(actor, "refund", d.record_id, decision["reason"],
                                 item.fingerprint, self._refund_payload(d),
                                 decision.get("detail"))
            else:  # dispute
                order = self.orders[self.by_payment[d.payment_id]]
                self._dispute_seq += 1
                self._append("dispute_opened", actor, {
                    "dispute_id": f"disp-{self._dispute_seq:04d}",
                    "kind": "cross_store_refund",
                    "ref": d.refund_id,
                    "campaign_id": order.campaign_id,
                    "reason": "跨店退款：退款发起方与补贴收取方不一致，需审计判定承担企业",
                    "detail": {**decision["detail"], "payment_id": d.payment_id,
                               "refund_amount": d.amount, "record_id": d.record_id,
                               "refund_id": d.refund_id},
                })

    def _accept_quarantined(self, ref: str, actor: str, resolution: dict) -> None:
        item = self.quarantine.get(ref)
        if item is None:
            raise RuleViolation(f"隔离记录 {ref} 不存在")
        if item["record_kind"] != "redemption":
            raise RuleViolation("仅核销隔离记录可人工受理")
        p = item["payload"]
        d = RedemptionData(
            record_id=p["record_id"], redemption_id=p["redemption_id"],
            campaign_id=p["campaign_id"], merchant_id=p["merchant_id"],
            payment_id=p["payment_id"], coupon_code=p["coupon_code"],
            amount=Decimal(str(p["amount"])), order_content=p.get("order_content", {}),
            occurred_at=date.fromisoformat(p["occurred_at"]),
        )
        decision = self._evaluate_redemption(d)
        if decision["action"] != "accept":
            why = decision.get("reason") or decision.get("wait")
            raise RuleViolation(f"争议解锁后仍不满足受理条件：{why}")
        self._append("quarantine_released", actor, {
            "record_id": ref, "redemption_id": d.redemption_id,
            "note": resolution.get("note", ""),
        })
        self._append("redemption_accepted", actor, {
            "redemption_id": d.redemption_id, "record_id": d.record_id,
            "campaign_id": d.campaign_id, "merchant_id": d.merchant_id,
            "payment_id": d.payment_id, "coupon_code": d.coupon_code,
            "amount": d.amount, "subsidy": decision["subsidy"],
            "batch_id": decision["batch_id"], "fingerprint": item["fingerprint"],
            "manual": True,
            "rule_path": self._rule_text(self.campaigns[d.campaign_id]),
        })

    # ------------------------------------------------------------ 事件应用

    def _append(self, kind: str, actor: str, payload: dict[str, Any]) -> None:
        event = self.store.append(kind, actor, to_jsonable(payload))
        self._apply(event, persist=True)

    def _apply(self, event: Any, persist: bool) -> None:
        p = event.payload
        kind = event.kind
        if kind == "budget_registered":
            self.budgets[p["budget_id"]] = AnnualBudget(total=Decimal(str(p["total"])))
        elif kind == "campaign_registered":
            spec = CampaignSpec(
                campaign_id=p["campaign_id"], name=p["name"],
                discount_rate=Decimal(str(p["discount_rate"])),
                settlement_day=date.fromisoformat(p["settlement_day"]),
                budget_id=p["budget_id"],
                standalone_budget=(
                    Decimal(str(p["budget_total"])) if p["budget_kind"] == "standalone" else None
                ),
                max_subsidy_per_order=(
                    Decimal(str(p["max_subsidy_per_order"]))
                    if p.get("max_subsidy_per_order") else None
                ),
                order_dedup_field=p["order_dedup_field"],
                merchants=frozenset(p["merchants"]),
            )
            self.campaigns[p["campaign_id"]] = Campaign(
                spec=spec, budget_total=Decimal(str(p["budget_total"])),
                budget_kind=p["budget_kind"],
            )
        elif kind == "batch_registered":
            spec = BatchSpec(
                batch_id=p["batch_id"], campaign_id=p["campaign_id"],
                codes=frozenset(p["codes"]), subsidy_quota=Decimal(str(p["subsidy_quota"])),
            )
            self.batches[p["batch_id"]] = BatchState(spec=spec)
        elif kind == "payment_received":
            self.payments[p["payment_id"]] = PaymentData(
                record_id=p["record_id"], payment_id=p["payment_id"],
                merchant_id=p["merchant_id"], amount=Decimal(str(p["amount"])),
                occurred_at=date.fromisoformat(p["occurred_at"]),
                source=p.get("source", "unionpay"),
            )
            self.records[("payment", p["record_id"])] = p["fingerprint"]
        elif kind == "redemption_received":
            self.records[("redemption", p["record_id"])] = p["fingerprint"]
            self._register_pending_redemption(p, p["fingerprint"])
        elif kind == "refund_received":
            self.records[("refund", p["record_id"])] = p["fingerprint"]
            self.refunds[p["refund_id"]] = {
                "refund_id": p["refund_id"], "record_id": p["record_id"],
                "payment_id": p["payment_id"], "amount": Decimal(str(p["amount"])),
                "merchant_id": p["merchant_id"],
                "occurred_at": p["occurred_at"], "status": "pending",
            }
            self._register_pending_refund(p, p["fingerprint"])
        elif kind == "redemption_accepted":
            self._apply_accepted(p, event.seq)
        elif kind == "refund_applied":
            self._apply_refund_reducer(p, event)
        elif kind in ("redemption_quarantined", "refund_quarantined", "payment_quarantined"):
            self.quarantine[p["record_ref"]] = p
            refund_id = p.get("payload", {}).get("refund_id")
            if refund_id and refund_id in self.refunds:
                self.refunds[refund_id]["status"] = "quarantined"
        elif kind == "redemption_rejected":
            self.rejected[p["record_id"]] = p
        elif kind == "dispute_opened":
            self._apply_dispute_opened(p, event.seq)
        elif kind == "dispute_resolved":
            self._apply_dispute_resolved(p, event.seq)
        elif kind == "window_sealed":
            self._apply_seal(p)
        elif kind == "late_order_confirmed":
            self._apply_late_confirmed(p, event.seq)
        elif kind == "quarantine_released":
            self.quarantine.pop(p["record_id"], None)

    def _register_pending_redemption(self, p: dict, fp: str) -> None:
        data = RedemptionData(
            record_id=p["record_id"], redemption_id=p["redemption_id"],
            campaign_id=p["campaign_id"], merchant_id=p["merchant_id"],
            payment_id=p["payment_id"], coupon_code=p["coupon_code"],
            amount=Decimal(str(p["amount"])), order_content=p.get("order_content", {}),
            occurred_at=date.fromisoformat(p["occurred_at"]),
        )
        self.pending[("redemption", p["record_id"])] = PendingItem(
            "redemption", p["record_id"], data, "unknown", fp)

    def _register_pending_refund(self, p: dict, fp: str) -> None:
        data = RefundData(
            record_id=p["record_id"], refund_id=p["refund_id"],
            payment_id=p["payment_id"], amount=Decimal(str(p["amount"])),
            merchant_id=p["merchant_id"], occurred_at=date.fromisoformat(p["occurred_at"]),
        )
        self.pending[("refund", p["record_id"])] = PendingItem(
            "refund", p["record_id"], data, "unknown", fp)

    def _apply_accepted(self, p: dict, seq: int) -> None:
        campaign = self.campaigns[p["campaign_id"]]
        subsidy = Decimal(str(p["subsidy"]))
        order = OrderState(
            redemption_id=p["redemption_id"], record_id=p["record_id"],
            campaign_id=p["campaign_id"], merchant_id=p["merchant_id"],
            payment_id=p["payment_id"], coupon_code=p["coupon_code"],
            amount=Decimal(str(p["amount"])), subsidy=subsidy,
            fingerprint=p["fingerprint"], accepted_seq=seq, manual=p.get("manual", False),
        )
        order.status = "late_pending" if campaign.sealed else "accepted"
        order.frozen = subsidy
        self.orders[p["redemption_id"]] = order
        self.by_payment[p["payment_id"]] = p["redemption_id"]
        self.spent_keys[p["payment_id"]] = (p["campaign_id"], p["redemption_id"])
        self.pending.pop(("redemption", p["record_id"]), None)

        campaign.frozen += subsidy
        self.batches[p["batch_id"]].frozen += subsidy
        self.batches[p["batch_id"]].used_codes.add(p["coupon_code"])
        if campaign.budget_kind == "shared":
            self.budgets[campaign.spec.budget_id].frozen += subsidy

    def _apply_refund_reducer(self, p: dict, event: Any) -> None:
        order = self.orders[self.by_payment[p["payment_id"]]]
        rec = self.refunds[p["refund_id"]]
        refund_amount = Decimal(str(p["refund_amount"]))
        campaign = self.campaigns[order.campaign_id]
        batch = self._batch_for_code(order.campaign_id, order.coupon_code)
        ratio = refund_amount / order.amount
        recovered = min(_share(order.subsidy * ratio), order.subsidy - order.recovered - order.recovered_post)
        order.refunded_amount += refund_amount

        adjustment: dict[str, Any] | None = None
        if order.status in ("accepted", "late_pending"):
            # 款项尚在冻结中：直接释放对应冻结额度
            order.recovered += recovered
            campaign.frozen -= recovered
            batch.frozen -= recovered
            if campaign.budget_kind == "shared":
                self.budgets[campaign.spec.budget_id].frozen -= recovered
            order.frozen -= recovered
        else:  # settled | late_settled：补贴已拨付，不回冲已结算数字，
            # 而是累计"应返还"（recovered_post）并形成封账后调整，快照保持不变。
            order.recovered_post += recovered
            campaign.recovered += recovered
            batch.recovered += recovered
            if campaign.budget_kind == "shared":
                self.budgets[campaign.spec.budget_id].recovered += recovered
            adj_type = "late_refund_recovery" if order.status == "late_settled" else "refund_recovery"
            adjustment = {
                "type": adj_type,
                "campaign_id": order.campaign_id,
                "snapshot_id": campaign.sealed_snapshot_id,
                "merchant_id": p.get("responsible_merchant", order.merchant_id),
                "redemption_id": order.redemption_id,
                "refund_id": p["refund_id"],
                "recovered": recovered,
                "event_seq": event.seq,
                "note": "封账后退款冲回，快照不变，由承担企业返还",
            }
        rec["status"] = "applied"
        rec["applied_seq"] = event.seq
        rec["recovered"] = recovered
        rec["responsible_merchant"] = p.get("responsible_merchant", order.merchant_id)
        if adjustment:
            self.adjustments.append(adjustment)

    def _apply_dispute_opened(self, p: dict, seq: int) -> None:
        self._dispute_seq = max(self._dispute_seq, int(p["dispute_id"].split("-")[-1]))
        dispute = Dispute(
            dispute_id=p["dispute_id"], kind=p["kind"], ref=_dispute_ref(p),
            campaign_id=p.get("campaign_id"), reason=p["reason"], opened_seq=seq,
        )
        self.disputes[p["dispute_id"]] = dispute
        if p["kind"] == "cross_store_refund":
            detail = p["detail"]
            order = self.orders[self.by_payment[detail["payment_id"]]]
            order.dispute_id = p["dispute_id"]
            rec = self.refunds[detail["refund_id"]]
            rec["status"] = "disputed"
            rec["dispute_id"] = p["dispute_id"]
            rec["subsidy_merchant"] = detail["subsidy_merchant"]
            self.pending.pop(("refund", detail["record_id"]), None)

    def _apply_dispute_resolved(self, p: dict, seq: int) -> None:
        dispute = self.disputes[p["dispute_id"]]
        dispute.status = "resolved"
        dispute.resolution = p["resolution"]
        dispute.resolved_seq = seq
        order = next((o for o in self.orders.values()
                      if o.dispute_id == dispute.dispute_id), None)
        if order is not None:
            order.dispute_id = None
            # 封账时因争议挂起的订单，解锁后进入补报通道由财务确认净额
            campaign = self.campaigns[order.campaign_id]
            if campaign.sealed and order.status == "accepted":
                order.status = "late_pending"

    def _apply_seal(self, p: dict) -> None:
        campaign = self.campaigns[p["campaign_id"]]
        amount = Decimal(str(p["frozen_settled"]))
        campaign.frozen -= amount
        campaign.settled += amount
        if campaign.budget_kind == "shared":
            pool = self.budgets[campaign.spec.budget_id]
            pool.frozen -= amount
            pool.settled += amount
        for order in self.orders.values():
            if (order.campaign_id != p["campaign_id"] or order.status != "accepted"
                    or order.dispute_id):
                continue
            order.status = "settled"
            order.settled = order.frozen
            order.frozen = Decimal("0.00")
            order.sealed_in = p["snapshot_id"]
            batch = self._batch_for_code(order.campaign_id, order.coupon_code)
            batch.frozen -= order.settled
            batch.settled += order.settled
        campaign.sealed_snapshot_id = p["snapshot_id"]

    def _apply_late_confirmed(self, p: dict, seq: int) -> None:
        order = self.orders[p["redemption_id"]]
        campaign = self.campaigns[order.campaign_id]
        batch = self._batch_for_code(order.campaign_id, order.coupon_code)
        net = order.frozen  # 已扣减封账后退款的净额
        order.status = "late_settled"
        order.settled = net
        order.frozen = Decimal("0.00")
        order.sealed_in = p["snapshot_id"] + "#late"
        campaign.frozen -= net
        campaign.late_settled += net
        if campaign.budget_kind == "shared":
            pool = self.budgets[campaign.spec.budget_id]
            pool.frozen -= net
            pool.late_settled += net
        batch.frozen -= net
        batch.late_settled += net
        self.adjustments.append({
            "type": "late_redemption",
            "campaign_id": order.campaign_id,
            "snapshot_id": p["snapshot_id"],
            "merchant_id": order.merchant_id,
            "redemption_id": order.redemption_id,
            "subsidy": net,
            "subsidy_original": Decimal(str(p["subsidy_original"])),
            "event_seq": seq,
            "note": p.get("note", "封账后补报核销，财务确认后另行拨付"),
        })

    # ------------------------------------------------------------ 隔离区

    def _quarantine(self, actor: str, kind: str, record_id: str, reason: str,
                    fp: str, payload: dict, detail: dict | None = None,
                    open_dispute: bool = True) -> str:
        record_ref = record_id
        self._append(f"{kind}_quarantined", actor, {
            "record_kind": kind, "record_id": record_id, "record_ref": record_ref,
            "reason": reason, "fingerprint": fp, "payload": payload,
            "detail": detail or {},
        })
        self.pending.pop((kind, record_id), None)
        if open_dispute and reason in (
            "payment_merchant_mismatch", "amount_mismatch", "refund_exceeds_order",
        ):
            self._dispute_seq += 1
            self._append("dispute_opened", actor, {
                "dispute_id": f"disp-{self._dispute_seq:04d}",
                "kind": "quarantined_record",
                "ref": record_ref,
                "campaign_id": payload.get("campaign_id"),
                "reason": f"{reason}：记录与既有事实矛盾，需审计解锁",
                "detail": detail or {},
            })
        return "quarantined"

    def _quarantine_retransmit(self, actor: str, kind: str, record_id: str,
                               payload: dict, fp: str | None = None) -> str:
        """同记录号但内容指纹不同：隔离重传，原记录保持有效。"""
        fp = fp or content_fingerprint(payload)
        ref = f"{record_id}::retrans-{fp[:10]}"
        self._append(f"{kind}_quarantined", actor, {
            "record_kind": kind, "record_id": ref, "record_ref": ref,
            "original_record_id": record_id,
            "reason": "fingerprint_conflict",
            "fingerprint": fp, "payload": payload,
            "detail": {"original_fingerprint": self.records.get((kind, record_id))},
        })
        self._dispute_seq += 1
        self._append("dispute_opened", actor, {
            "dispute_id": f"disp-{self._dispute_seq:04d}",
            "kind": "quarantined_record",
            "ref": ref,
            "campaign_id": payload.get("campaign_id"),
            "reason": "同记录号重传内容指纹不一致：金额相同也不能覆盖原记录，需审计解锁",
            "detail": {},
        })
        return "quarantined"

    # ------------------------------------------------------------ 辅助方法

    def _dedupe_gate(self, key: tuple[str, str], fp: str) -> str | None:
        existing = self.records.get(key)
        if existing is None:
            return None
        return "ignored_duplicate" if existing == fp else "fingerprint_conflict"

    def _calc_subsidy(self, campaign: Campaign, amount: Decimal) -> Decimal:
        subsidy = _share(amount * campaign.spec.discount_rate)
        if campaign.spec.max_subsidy_per_order is not None:
            subsidy = min(subsidy, campaign.spec.max_subsidy_per_order)
        return subsidy

    def _available_for(self, campaign: Campaign) -> Decimal:
        """活动当前可用额度：共享池按池口径，独立额度按活动口径。"""
        if campaign.budget_kind == "shared":
            pool = self.budgets[campaign.spec.budget_id]
            return pool.total - pool.frozen - pool.settled - pool.late_settled
        return campaign.available_budget

    def _batch_for_code(self, campaign_id: str, code: str) -> BatchState | None:
        for b in self.batches.values():
            if b.spec.campaign_id == campaign_id and code in b.spec.codes:
                return b
        return None

    def _campaign(self, campaign_id: str) -> Campaign:
        if campaign_id not in self.campaigns:
            raise RuleViolation(f"活动 {campaign_id} 不存在")
        return self.campaigns[campaign_id]

    def _order(self, redemption_id: str) -> OrderState:
        if redemption_id not in self.orders:
            raise RuleViolation(f"核销单 {redemption_id} 不存在")
        return self.orders[redemption_id]

    def _result_for_record(self, key: tuple[str, str]) -> str:
        if key in self.pending:
            return "pending"
        record_id = key[1]
        if record_id in self.quarantine or any(
            q.get("original_record_id") == record_id for q in self.quarantine.values()
        ):
            return "quarantined"
        if record_id in self.rejected:
            return "rejected"
        return "accepted"

    def _require(self, user: User, role: Role, action: str) -> None:
        if not user.can(role):
            raise PermissionDenied(
                f"{user.name} 缺少 {role.value} 角色，无权执行：{action}"
            )

    def _rule_text(self, c: Campaign) -> str:
        cap = c.spec.max_subsidy_per_order
        pool = f"共享年度预算池 {c.spec.budget_id}" if c.budget_kind == "shared" else "活动独立额度"
        pct = format(c.spec.discount_rate * 100, "f").rstrip("0").rstrip(".")
        return (f"按消费金额 {pct}% 补贴"
                + (f"，单笔封顶 {cap} 元" if cap is not None else "")
                + f"；资金来源：{pool}；同一支付单全活动限享一次补贴")

    def _explain_order(self, o: OrderState) -> dict[str, Any]:
        c = self.campaigns[o.campaign_id]
        batch_id = next((bid for bid, b in self.batches.items()
                         if b.spec.campaign_id == o.campaign_id
                         and o.coupon_code in b.spec.codes), None)
        return {
            "rule": self._rule_text(c),
            "calc": f"{o.amount} × {c.spec.discount_rate} = {o.subsidy}",
            "batch_id": batch_id,
            "payment_id": o.payment_id,
            "accepted_event_seq": o.accepted_seq,
            "sealed_in": o.sealed_in,
            "manual": o.manual,
        }

    def _trace_window_line(self, line: dict) -> dict[str, Any]:
        orders = [self.get_order(rid) for rid in line["order_ids"]]
        payment_ids = {o["payment_id"] for o in orders}
        refunds = [
            {k: (str(v) if isinstance(v, Decimal) else v)
             for k, v in rec.items() if k != "subsidy_merchant"}
            for rec in self.refunds.values() if rec["payment_id"] in payment_ids
        ]
        payments = [
            {
                "record_id": self.payments[pid].record_id,
                "payment_id": pid,
                "merchant_id": self.payments[pid].merchant_id,
                "amount": self.payments[pid].amount,
            }
            for pid in sorted(payment_ids)
        ]
        return {
            "source": "window",
            "merchant_id": line["merchant_id"],
            "gross": line["gross"],
            "subsidy": line["subsidy"],
            "orders": orders,
            "payments": payments,
            "refunds": refunds,
        }

    def _evidence(self, redemption_id: str | None) -> dict[str, Any]:
        if not redemption_id or redemption_id not in self.orders:
            return {}
        o = self.orders[redemption_id]
        pay = self.payments.get(o.payment_id)
        return {
            "order": self.get_order(redemption_id),
            "payment": None if pay is None else {
                "record_id": pay.record_id, "payment_id": pay.payment_id,
                "merchant_id": pay.merchant_id, "amount": pay.amount,
                "occurred_at": pay.occurred_at,
            },
        }

    def _decode_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        snap = self.store.read_snapshot(snapshot_id)
        for key in ("frozen_before", "settled_before", "settled_this_window",
                    "frozen_after", "settled_after"):
            snap["budget"][key] = Decimal(str(snap["budget"][key]))
        for key in ("budget_total", "max_subsidy_per_order"):
            if snap["rule"].get(key) is not None:
                snap["rule"][key] = Decimal(str(snap["rule"][key]))
        for line in snap["merchants"]:
            line["gross"] = Decimal(str(line["gross"]))
            line["subsidy"] = Decimal(str(line["subsidy"]))
        return snap

    # ------------------------------------------------------- payload 规整

    @staticmethod
    def _payment_payload(d: PaymentData) -> dict:
        return {
            "record_id": d.record_id, "payment_id": d.payment_id,
            "merchant_id": d.merchant_id, "amount": money(d.amount),
            "occurred_at": d.occurred_at, "source": d.source,
        }

    @staticmethod
    def _redemption_payload(d: RedemptionData) -> dict:
        return {
            "record_id": d.record_id, "redemption_id": d.redemption_id,
            "campaign_id": d.campaign_id, "merchant_id": d.merchant_id,
            "payment_id": d.payment_id, "coupon_code": d.coupon_code,
            "amount": money(d.amount), "order_content": d.order_content,
            "occurred_at": d.occurred_at,
        }

    @staticmethod
    def _refund_payload(d: RefundData) -> dict:
        return {
            "record_id": d.record_id, "refund_id": d.refund_id,
            "payment_id": d.payment_id, "amount": money(d.amount),
            "merchant_id": d.merchant_id, "occurred_at": d.occurred_at,
        }


def _dispute_ref(p: dict) -> str:
    return p["ref"]
