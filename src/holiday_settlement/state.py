"""结算状态机：事件溯源投影 + 业务命令处理。

核心恒等式（任何时刻、重启重放后都成立）::

    预算池总额 total = 可用 available + 冻结 frozen + 已结算 settled
    池冻结额        = 所有处于冻结态订单的补贴之和
    池已结算额      = 所有处于已结算态订单的补贴之和

金额生命周期：available --核销冻结--> frozen --窗口分账--> settled；
结算前退款 frozen -> available；封账后退款/审计追回 settled -> available，
同时留下一笔「下期追减」调整单用于解释，但钱在退款事件生效时即回池。

关键设计：
- 命令只做校验并产出事件；状态全部由 ``apply`` 投影得到，重启即重放。
- 封账快照不可变；迟到补报进隔离区，绝不回写快照。
- 渠道重传按「(来源, 渠道流水号) + 内容指纹」识别：同指纹幂等吸收，
  指纹不同（金额相同、订单内容不同）进隔离区。
- 一笔银联支付在任一活动领取补贴后即被占用，其他活动无法二次补贴。
- 核销先于支付、退款先于订单等乱序回执均可以到达，投影负责补齐挂起。
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from .errors import (
    BudgetExhaustedError,
    DisputeStateError,
    DuplicateCommandError,
    OrderStateError,
    ReferenceError,
    RuleConflictError,
    SettlementError,
    ValidationError,
)
from .records import (
    EV_BATCH_DEFINED,
    EV_BOOKS_CLOSED,
    EV_BUDGET_DEPOSITED,
    EV_ACTIVITY_DEFINED,
    EV_COMPANY_REGISTERED,
    EV_DISPUTE_RAISED,
    EV_DISPUTE_RESOLVED,
    EV_LATE_ARRIVAL_QUARANTINED,
    EV_PAYMENT_RECEIVED,
    EV_POST_CLOSE_ADJUSTMENT,
    EV_REDEMPTION_RECEIVED,
    EV_REPORT_RECEIVED,
    EV_REFUND_RECEIVED,
    EV_SUBSIDY_FROZEN,
    EV_SUBSIDY_RECOVERED,
    EV_SUBSIDY_RELEASED,
    EV_WINDOW_SETTLED,
)

EV_WINDOW_OPENED = "window_opened"

# 订单状态
ST_PENDING = "pending"            # 核销已到但支付未匹配（额度已冻结）
ST_FROZEN = "frozen"              # 核销+支付齐备，额度冻结可结算
ST_SETTLED = "settled"            # 已进入结算单
ST_REFUNDED = "refunded"          # 结算前退款，冻结已释放回池
ST_CLAWED_BACK = "clawed_back"    # 结算后退款/审计追回，补贴已回池
ST_RELEASED = "released"          # 审计放行（争议解除补贴）

FROZEN_LIKE = (ST_PENDING, ST_FROZEN)

# 隔离原因
Q_LATE_CLOSED = "late_after_close"          # 封账后补报
Q_RETRANSMIT_MISMATCH = "retransmit_diff"   # 同渠道流水号但内容指纹不同
Q_UNKNOWN_REF = "bad_reference"             # 引用了不存在的活动/批次

# 调整单类型
ADJ_CLAWBACK = "clawback"
ADJ_DISPUTE_RELEASE = "dispute_release"
ADJ_DISPUTE_REASSIGN = "dispute_reassign"

# 不参与订单内容指纹的传输性字段
_NON_CONTENT_KEYS = {"key", "actor", "note", "memo", "command_fingerprint"}


def fingerprint(value: dict[str, Any]) -> str:
    """记录内容指纹：传输字段（key/actor/备注）不参与。"""
    body = {k: v for k, v in value.items() if k not in _NON_CONTENT_KEYS}
    raw = json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def available_of(pool: dict) -> int:
    """可用 = 总额 - 冻结 - 已结算（恒等式推导，不单独存储）。"""
    return pool["total_cents"] - pool["frozen_cents"] - pool["settled_cents"]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require(payload: dict, *fields: str) -> None:
    for name in fields:
        if name not in payload or payload[name] in (None, ""):
            raise ValidationError(f"命令缺少必填字段：{name}")


class SettlementState:
    """纯内存投影；持久化由 Journal 重放保证。"""

    def __init__(self) -> None:
        self.companies: dict[str, dict] = {}
        self.activities: dict[str, dict] = {}
        self.batches: dict[str, dict] = {}
        self.pools: dict[str, dict] = {}
        self.orders: dict[str, dict] = {}
        self.periods: dict[str, dict] = {}
        self.settlements: dict[str, dict] = {}
        self.snapshots: dict[str, dict] = {}
        self.disputes: dict[str, dict] = {}
        self.adjustments: list[dict] = []
        self.quarantine: list[dict] = []
        self.reports: list[dict] = []
        # (source, channel_ref) -> {order_id, fingerprint, seq}
        self.channel_index: dict[tuple[str, str], dict] = {}
        # 券码全局唯一：code -> batch_id
        self.code_index: dict[str, str] = {}
        # 银联支付占用：payment_reference -> {activity_id, order_id}
        self.consumed: dict[str, dict] = {}
        # 乱序暂存：退款先于订单核销到达
        self.pending_refunds: dict[str, list[dict]] = defaultdict(list)
        # 命令幂等键 -> {seq, fingerprint}
        self.idem: dict[str, dict] = {}
        self.head_seq = 0
        self.head_digest = ""

    # ==================================================================
    # 命令入口
    # ==================================================================

    def handle(
        self, cmd_type: str, key: str, payload: dict
    ) -> list[tuple[str, dict]]:
        """校验命令，返回一个或多个待追加事件（类型, 负载）。"""
        fp = fingerprint(payload)
        if key in self.idem:
            seen = self.idem[key]
            if seen["fingerprint"] == fp:
                return []  # 完全相同的重放：幂等吸收
            raise DuplicateCommandError(
                f"幂等键 {key} 曾用于其他内容（原事件 #{seen['seq']}），"
                "金额相同但订单内容不同的重传必须隔离"
            )
        handler = getattr(self, f"_cmd_{cmd_type}", None)
        if handler is None:
            raise ValidationError(f"未知命令类型：{cmd_type}")
        events = handler(key, dict(payload))
        # 每条事件都携带命令指纹，供重放时重建幂等索引
        return [(t, {**p, "command_fingerprint": fp}) for t, p in events]

    # ------------------------------------------------------------------
    # 企业 / 预算 / 活动规则 / 券批次
    # ------------------------------------------------------------------

    def _cmd_register_company(self, key: str, p: dict) -> list[tuple[str, dict]]:
        _require(p, "company_id", "name")
        cid = p["company_id"]
        if cid in self.companies:
            raise DuplicateCommandError(f"企业已登记：{cid}")
        return [(EV_COMPANY_REGISTERED, {"company_id": cid, "name": p["name"]})]

    def _cmd_deposit_budget(self, key: str, p: dict) -> list[tuple[str, dict]]:
        _require(p, "pool_id", "amount_cents")
        amount = int(p["amount_cents"])
        if amount <= 0:
            raise ValidationError("预算金额必须为正")
        total_after = self.pools.get(p["pool_id"], {}).get("total_cents", 0) + amount
        return [
            (
                EV_BUDGET_DEPOSITED,
                {
                    "pool_id": p["pool_id"],
                    "amount_cents": amount,
                    "total_after_cents": total_after,
                    "year": p.get("year"),
                },
            )
        ]

    def _cmd_define_activity(self, key: str, p: dict) -> list[tuple[str, dict]]:
        _require(p, "activity_id", "name", "pool_id")
        aid = p["activity_id"]
        if aid in self.activities:
            raise DuplicateCommandError(f"活动已定义：{aid}")
        if p["pool_id"] not in self.pools:
            raise ReferenceError(f"预算池不存在：{p['pool_id']}（请先由财务存入预算）")
        if int(p.get("ratio_permille", 1000)) <= 0:
            raise ValidationError("补贴比例必须为正整数（千分比）")
        data = {
            "activity_id": aid,
            "name": p["name"],
            "pool_id": p["pool_id"],
            "activity_cap_cents": int(p["activity_cap_cents"]) if p.get("activity_cap_cents") is not None else None,
            "ratio_permille": int(p.get("ratio_permille", 1000)),
            "per_order_cap_cents": int(p["per_order_cap_cents"]) if p.get("per_order_cap_cents") is not None else None,
            "rule_event_seq": self.head_seq + 1,
        }
        return [(EV_ACTIVITY_DEFINED, data)]

    def _cmd_define_coupon_batch(self, key: str, p: dict) -> list[tuple[str, dict]]:
        _require(p, "batch_id", "activity_id")
        bid = p["batch_id"]
        if bid in self.batches:
            raise DuplicateCommandError(f"券批次已定义：{bid}")
        if p["activity_id"] not in self.activities:
            raise ReferenceError(f"活动不存在：{p['activity_id']}")
        if p.get("face_value_cents") is not None and int(p["face_value_cents"]) <= 0:
            raise ValidationError("券面值必须为正")
        data = {
            "batch_id": bid,
            "activity_id": p["activity_id"],
            "face_value_cents": int(p["face_value_cents"]) if p.get("face_value_cents") is not None else None,
            "category": p.get("category", ""),
            "rule_event_seq": self.head_seq + 1,
        }
        return [(EV_BATCH_DEFINED, data)]

    def _cmd_open_window(self, key: str, p: dict) -> list[tuple[str, dict]]:
        _require(p, "period_id", "activity_ids")
        pid = p["period_id"]
        if pid in self.periods:
            raise DuplicateCommandError(f"结算窗口已存在：{pid}")
        aids = list(p["activity_ids"])
        if not aids:
            raise ValidationError("窗口至少覆盖一个活动")
        for aid in aids:
            if aid not in self.activities:
                raise ReferenceError(f"活动不存在：{aid}")
        return [
            (
                EV_WINDOW_OPENED,
                {"period_id": pid, "activity_ids": aids, "opened_at": _now()},
            )
        ]

    # ------------------------------------------------------------------
    # 销售/客流批次报告（仅信息留档，不参与金额）
    # ------------------------------------------------------------------

    def _cmd_receive_report(self, key: str, p: dict) -> list[tuple[str, dict]]:
        _require(p, "report_id", "kind", "source", "channel_ref")
        if p["kind"] not in ("sales", "footfall"):
            raise ValidationError("报告 kind 只能是 sales / footfall")
        seen = self._channel_seen(p, "")
        if seen == "duplicate":
            return []
        if seen == Q_RETRANSMIT_MISMATCH:
            return [self._quarantine("receive_report", key, p, seen)]
        return [
            (
                EV_REPORT_RECEIVED,
                {
                    "report_id": p["report_id"],
                    "kind": p["kind"],
                    "source": p["source"],
                    "channel_ref": p["channel_ref"],
                    "period_id": p.get("period_id"),
                    "metrics": p.get("metrics", {}),
                    "fingerprint": fingerprint(p),
                },
            )
        ]

    # ------------------------------------------------------------------
    # 核销 / 支付 / 退款（乱序到达）
    # ------------------------------------------------------------------

    def _channel_seen(self, p: dict, order_id: str) -> str | None:
        """duplicate=同内容重传（幂等）；retransmit_diff=必须隔离。"""
        ref = p.get("channel_ref")
        if not ref:
            return None
        idx = self.channel_index.get((p.get("source", "unknown"), ref))
        if idx is None:
            return None
        if idx["fingerprint"] == fingerprint(p) and (
            not order_id or idx["order_id"] == order_id
        ):
            return "duplicate"
        return Q_RETRANSMIT_MISMATCH

    def _activity_committed(self, activity_id: str) -> int:
        return sum(
            o["subsidy_cents"]
            for o in self.orders.values()
            if o.get("activity_id") == activity_id
            and o["state"] in (ST_FROZEN, ST_SETTLED, ST_PENDING)
        )

    def _cmd_receive_redemption(self, key: str, p: dict) -> list[tuple[str, dict]]:
        _require(
            p, "order_id", "company_id", "batch_id", "code", "gross_cents",
            "source", "channel_ref",
        )
        p["gross_cents"] = int(p["gross_cents"])
        if p["gross_cents"] <= 0:
            raise ValidationError("订单金额必须为正")
        order_id = p["order_id"]

        seen = self._channel_seen(p, order_id)
        if seen == "duplicate":
            return []
        if seen == Q_RETRANSMIT_MISMATCH:
            return [self._quarantine("receive_redemption", key, p, seen)]

        if p["batch_id"] not in self.batches:
            return [self._quarantine("receive_redemption", key, p, Q_UNKNOWN_REF)]
        batch = self.batches[p["batch_id"]]
        activity = self.activities[batch["activity_id"]]

        if p["company_id"] not in self.companies:
            raise ReferenceError(f"企业不存在：{p['company_id']}")

        period_id = p.get("period_id")
        if period_id and self._is_closed(period_id):
            return [self._quarantine("receive_redemption", key, p, Q_LATE_CLOSED)]

        order = self.orders.get(order_id)
        if order and order.get("redemption"):
            raise DuplicateCommandError(f"订单 {order_id} 已有核销记录")
        if p["code"] in self.code_index:
            raise RuleConflictError(
                f"券码 {p['code']} 已在批次 {self.code_index[p['code']]} 核销，"
                "一张券码只能核销一次"
            )

        subsidy = self._calc_subsidy(activity, batch, p["gross_cents"])
        pool = self.pools[activity["pool_id"]]
        if available_of(pool) < subsidy:
            raise BudgetExhaustedError(
                f"预算池 {activity['pool_id']} 可用不足：需 {subsidy}，"
                f"余 {available_of(pool)}（共享年度预算可能已被其他活动冻结）"
            )
        cap = activity.get("activity_cap_cents")
        if cap is not None and self._activity_committed(activity["activity_id"]) + subsidy > cap:
            raise BudgetExhaustedError(
                f"活动 {activity['activity_id']} 累计补贴上限 {cap} 分，"
                f"本次 {subsidy} 分将超限"
            )

        # 支付可能先于核销到达；若支付引用已被别的活动占用，立即拒绝二次补贴
        payment_ref = p.get("payment_reference")
        if not payment_ref and order and order.get("payment"):
            payment_ref = order["payment"]["payment_reference"]
        if payment_ref:
            occupied = self.consumed.get(payment_ref)
            if occupied and occupied["activity_id"] != activity["activity_id"]:
                raise RuleConflictError(
                    f"银联支付 {payment_ref} 已在活动 {occupied['activity_id']} "
                    f"领取补贴，活动 {activity['activity_id']} 不得二次补贴"
                )

        return [
            (
                EV_REDEMPTION_RECEIVED,
                {
                    "order_id": order_id,
                    "company_id": p["company_id"],
                    "activity_id": activity["activity_id"],
                    "pool_id": activity["pool_id"],
                    "batch_id": p["batch_id"],
                    "code": p["code"],
                    "gross_cents": p["gross_cents"],
                    "subsidy_cents": subsidy,
                    "period_id": period_id,
                    "source": p["source"],
                    "channel_ref": p["channel_ref"],
                    "fingerprint": fingerprint(p),
                    "payment_reference": p.get("payment_reference"),
                    "rule_event_seq": activity["rule_event_seq"],
                    "batch_rule_event_seq": batch["rule_event_seq"],
                },
            ),
            (
                EV_SUBSIDY_FROZEN,
                {
                    "order_id": order_id,
                    "pool_id": activity["pool_id"],
                    "activity_id": activity["activity_id"],
                    "amount_cents": subsidy,
                    "reason": "券码核销即按活动规则冻结可用额度",
                },
            ),
        ]

    def _cmd_receive_payment(self, key: str, p: dict) -> list[tuple[str, dict]]:
        _require(
            p, "order_id", "company_id", "payment_reference", "amount_cents",
            "source", "channel_ref",
        )
        p["amount_cents"] = int(p["amount_cents"])
        if p["amount_cents"] <= 0:
            raise ValidationError("支付金额必须为正")
        order_id = p["order_id"]

        seen = self._channel_seen(p, order_id)
        if seen == "duplicate":
            return []
        if seen == Q_RETRANSMIT_MISMATCH:
            return [self._quarantine("receive_payment", key, p, seen)]
        if p["company_id"] not in self.companies:
            raise ReferenceError(f"企业不存在：{p['company_id']}")

        period_id = p.get("period_id")
        order = self.orders.get(order_id)
        if period_id and self._is_closed(period_id):
            return [self._quarantine("receive_payment", key, p, Q_LATE_CLOSED)]

        if order and order.get("activity_id"):
            occupied = self.consumed.get(p["payment_reference"])
            if occupied and occupied["activity_id"] != order["activity_id"]:
                raise RuleConflictError(
                    f"银联支付 {p['payment_reference']} 已在活动 "
                    f"{occupied['activity_id']} 领取补贴，活动 "
                    f"{order['activity_id']} 不得二次补贴"
                )
        elif self.consumed.get(p["payment_reference"]):
            occ = self.consumed[p["payment_reference"]]
            if occ["order_id"] != order_id:
                raise RuleConflictError(
                    f"银联支付 {p['payment_reference']} 已被订单 {occ['order_id']} "
                    "占用，禁止重复补贴"
                )

        return [
            (
                EV_PAYMENT_RECEIVED,
                {
                    "order_id": order_id,
                    "company_id": p["company_id"],
                    "payment_reference": p["payment_reference"],
                    "amount_cents": p["amount_cents"],
                    "period_id": period_id,
                    "source": p["source"],
                    "channel_ref": p["channel_ref"],
                    "fingerprint": fingerprint(p),
                },
            )
        ]

    def _cmd_receive_refund(self, key: str, p: dict) -> list[tuple[str, dict]]:
        _require(p, "order_id", "refund_id", "amount_cents", "source", "channel_ref")
        p["amount_cents"] = int(p["amount_cents"])
        if p["amount_cents"] <= 0:
            raise ValidationError("退款金额必须为正")
        order_id = p["order_id"]

        seen = self._channel_seen(p, order_id)
        if seen == "duplicate":
            return []
        if seen == Q_RETRANSMIT_MISMATCH:
            return [self._quarantine("receive_refund", key, p, seen)]

        order = self.orders.get(order_id)
        if order is None or not order.get("redemption"):
            # 乱序回执：退款先于核销到达，挂起等待订单补齐
            return [
                (
                    EV_REFUND_RECEIVED,
                    {
                        "order_id": order_id,
                        "refund_id": p["refund_id"],
                        "amount_cents": p["amount_cents"],
                        "refunding_company_id": p.get("company_id"),
                        "source": p["source"],
                        "channel_ref": p["channel_ref"],
                        "fingerprint": fingerprint(p),
                        "effect": "parked",
                    },
                )
            ]
        return self._refund_effects(p, order)

    def _refund_effects(self, p: dict, order: dict) -> list[tuple[str, dict]]:
        order_id = order["order_id"]
        if order.get("dispute_id"):
            raise OrderStateError(
                f"订单 {order_id} 存在未解锁争议单 {order['dispute_id']}，"
                "退款须由审计处理"
            )
        if order["state"] in (ST_REFUNDED, ST_CLAWED_BACK, ST_RELEASED):
            raise DuplicateCommandError(f"订单 {order_id} 已完成退款/放行")

        amount = p["amount_cents"]
        full = amount >= order["gross_cents"]
        cross = bool(p.get("company_id")) and p["company_id"] != order["company_id"]
        # 承担返还的企业：实际领取并应退回补贴的企业（跨店退款时关键）
        responsible = order["company_id"]
        base = {
            "order_id": order_id,
            "refund_id": p["refund_id"],
            "amount_cents": amount,
            "full_refund": full,
            "refunding_company_id": p.get("company_id"),
            "responsible_company_id": responsible,
            "cross_company": cross,
            "source": p["source"],
            "channel_ref": p["channel_ref"],
            "fingerprint": fingerprint(p),
        }
        settled_in = order.get("settlement_id")
        sealed = bool(
            settled_in
            and self.settlements.get(settled_in, {}).get("snapshot_id")
        )

        if order["state"] in FROZEN_LIKE:
            base["effect"] = "release_frozen"
            return [
                (EV_REFUND_RECEIVED, base),
                (
                    EV_SUBSIDY_RELEASED,
                    {
                        "order_id": order_id,
                        "pool_id": order["pool_id"],
                        "activity_id": order["activity_id"],
                        "amount_cents": order["subsidy_cents"],
                        "reason": "结算前退款，冻结额度释放回可用预算",
                    },
                ),
            ]

        if order["state"] != ST_SETTLED:
            raise OrderStateError(f"订单 {order_id} 状态 {order['state']} 不允许退款")

        if not sealed:
            # 结算单已生成但未封账：直接在本期更正，封账时定格更正后的数字
            base["effect"] = "clawback_unsealed"
            return [
                (EV_REFUND_RECEIVED, base),
                (
                    EV_SUBSIDY_RECOVERED,
                    {
                        "order_id": order_id,
                        "pool_id": order["pool_id"],
                        "activity_id": order["activity_id"],
                        "amount_cents": order["subsidy_cents"],
                        "settlement_id": settled_in,
                        "reason": "封账前退款，直接冲减本期分账",
                    },
                ),
            ]

        # 已封账：快照不动；钱回池，同时挂一笔下期追减调整单
        adj_id = f"ADJ-{p['refund_id']}"
        base["effect"] = "post_close_clawback"
        base["adjustment_id"] = adj_id
        return [
            (EV_REFUND_RECEIVED, base),
            (
                EV_POST_CLOSE_ADJUSTMENT,
                {
                    "adjustment_id": adj_id,
                    "type": ADJ_CLAWBACK,
                    "order_id": order_id,
                    "company_id": responsible,
                    "activity_id": order["activity_id"],
                    "pool_id": order["pool_id"],
                    "period_id": order.get("period_id"),
                    "snapshot_id": self.settlements[settled_in]["snapshot_id"],
                    "amount_cents": order["subsidy_cents"],
                    "reason": "封账后退款：快照不变，补贴回池并作为下期追减",
                    "status": "pending",
                },
            ),
        ]

    # ------------------------------------------------------------------
    # 结算窗口
    # ------------------------------------------------------------------

    def _cmd_settle_window(self, key: str, p: dict) -> list[tuple[str, dict]]:
        _require(p, "period_id")
        period_id = p["period_id"]
        period = self.periods.get(period_id)
        if period is None:
            raise ReferenceError(f"结算窗口不存在：{period_id}")
        if period["status"] != "open":
            raise OrderStateError(f"窗口 {period_id} 状态为 {period['status']}，不可再结算")

        entries: list[dict] = []
        excluded: list[dict] = []
        window_activities = set(period["activity_ids"])
        for order in self.orders.values():
            activity_id = order.get("activity_id")
            if activity_id not in window_activities:
                continue
            order_period = order.get("period_id")
            # 本窗口订单，或上期已结算/封账后才补齐支付的冻结结转订单
            in_window = (
                order_period == period_id
                or order_period is None
                or (
                    (pr := self.periods.get(order_period)) is not None
                    and pr["status"] in ("settled", "closed")
                )
            )
            if not in_window:
                continue
            if order.get("dispute_id"):
                excluded.append({"order_id": order["order_id"], "reason": "争议单未解锁"})
            elif order["state"] == ST_FROZEN:
                entries.append(self._split_entry(order, period_id))
            elif order["state"] == ST_PENDING and order_period == period_id:
                excluded.append(
                    {"order_id": order["order_id"], "state": order["state"],
                     "reason": "银联支付未匹配，额度保持冻结"}
                )
            # 已退款/已追回/已放行不计入分账

        totals_company: dict[str, dict] = self._empty_company_totals()
        totals_activity: dict[str, dict] = defaultdict(
            lambda: {"subsidy_cents": 0, "orders": 0}
        )
        for e in entries:
            c = totals_company[e["company_id"]]
            c["gross_cents"] += e["gross_cents"]
            c["subsidy_cents"] += e["subsidy_cents"]
            c["orders"] += 1
            totals_activity[e["activity_id"]]["subsidy_cents"] += e["subsidy_cents"]
            totals_activity[e["activity_id"]]["orders"] += 1

        # 上期封账后挂账、归属本窗口活动的调整单，在本窗口追减/改派
        applied = [
            adj["adjustment_id"]
            for adj in self.adjustments
            if adj["status"] == "pending" and adj["activity_id"] in period["activity_ids"]
        ]

        return [
            (
                EV_WINDOW_SETTLED,
                {
                    "settlement_id": f"ST-{period_id}",
                    "period_id": period_id,
                    "entries": entries,
                    "excluded": excluded,
                    "totals_company": dict(totals_company),
                    "totals_activity": dict(totals_activity),
                    "applied_adjustments": applied,
                    "generated_at": _now(),
                },
            )
        ]

    @staticmethod
    def _empty_company_totals() -> dict[str, dict]:
        return defaultdict(lambda: {"gross_cents": 0, "subsidy_cents": 0, "orders": 0})

    def _split_entry(self, order: dict, period_id: str) -> dict:
        return {
            "settlement_id": f"ST-{period_id}",
            "period_id": period_id,
            "order_id": order["order_id"],
            "company_id": order["company_id"],
            "activity_id": order["activity_id"],
            "pool_id": order["pool_id"],
            "batch_id": order["redemption"]["batch_id"],
            "code": order["redemption"]["code"],
            "gross_cents": order["gross_cents"],
            "subsidy_cents": order["subsidy_cents"],
            "payment_reference": order["payment"]["payment_reference"],
            "payment_event_seq": order["payment"]["seq"],
            "redemption_event_seq": order["redemption"]["seq"],
            "rule_event_seq": order["redemption"]["rule_event_seq"],
            "explanation": (
                f"券码 {order['redemption']['code']} 核销并经银联支付 "
                f"{order['payment']['payment_reference']} 匹配，按活动 "
                f"{order['activity_id']} 规则版本 #{order['redemption']['rule_event_seq']}"
                f"（比例/封顶见事件 #{order['redemption']['rule_event_seq']}）"
                f"计算补贴 {order['subsidy_cents']} 分，由企业 "
                f"{order['company_id']} 领取"
            ),
        }

    def _cmd_close_books(self, key: str, p: dict) -> list[tuple[str, dict]]:
        _require(p, "period_id")
        period_id = p["period_id"]
        period = self.periods.get(period_id)
        if period is None:
            raise ReferenceError(f"结算窗口不存在：{period_id}")
        if period["status"] != "settled":
            raise OrderStateError(f"窗口 {period_id} 尚未结算，不能封账")
        settlement = self.settlements[period["settlement_id"]]
        involved_pools = {
            self.activities[a]["pool_id"] for a in period["activity_ids"]
        }
        snapshot = {
            "snapshot_id": f"SNAP-{period_id}",
            "settlement_id": period["settlement_id"],
            "period_id": period_id,
            "activity_ids": list(period["activity_ids"]),
            "sealed_at": _now(),
            "head_seq": self.head_seq + 1,
            "head_digest": self.head_digest,
            "pool_balances": {
                pid: {
                    "total_cents": self.pools[pid]["total_cents"],
                    "frozen_cents": self.pools[pid]["frozen_cents"],
                    "settled_cents": self.pools[pid]["settled_cents"],
                    "available_cents": available_of(self.pools[pid]),
                    "recovered_cents": self.pools[pid]["recovered_cents"],
                }
                for pid in sorted(involved_pools)
            },
            "totals_company": settlement["totals_company"],
            "totals_activity": settlement["totals_activity"],
            "entries": list(settlement["entries"]),
            "clawbacks": list(settlement.get("clawbacks", [])),
            "applied_adjustments": list(settlement.get("applied_adjustments", [])),
            "rule_versions": {
                aid: self.activities[aid]["rule_event_seq"]
                for aid in period["activity_ids"]
            },
        }
        return [
            (
                EV_BOOKS_CLOSED,
                {
                    "period_id": period_id,
                    "settlement_id": period["settlement_id"],
                    "snapshot": snapshot,
                },
            )
        ]

    # ------------------------------------------------------------------
    # 争议单（只有审计能解锁）
    # ------------------------------------------------------------------

    def _cmd_raise_dispute(self, key: str, p: dict) -> list[tuple[str, dict]]:
        _require(p, "dispute_id", "order_id", "reason")
        order = self.orders.get(p["order_id"])
        if order is None:
            raise ReferenceError(f"订单不存在：{p['order_id']}")
        if p["dispute_id"] in self.disputes:
            raise DuplicateCommandError(f"争议单已存在：{p['dispute_id']}")
        if order.get("dispute_id"):
            raise DisputeStateError(f"订单已有争议单 {order['dispute_id']}")
        snapshot_id = None
        if order.get("settlement_id"):
            snapshot_id = self.settlements[order["settlement_id"]].get("snapshot_id")
        return [
            (
                EV_DISPUTE_RAISED,
                {
                    "dispute_id": p["dispute_id"],
                    "order_id": p["order_id"],
                    "reason": p["reason"],
                    "snapshot_id": snapshot_id,
                },
            )
        ]

    def _cmd_resolve_dispute(self, key: str, p: dict) -> list[tuple[str, dict]]:
        _require(p, "dispute_id", "decision")
        d = self.disputes.get(p["dispute_id"])
        if d is None:
            raise ReferenceError(f"争议单不存在：{p['dispute_id']}")
        if d["status"] != "open":
            raise DisputeStateError(f"争议单 {p['dispute_id']} 已了结")
        decision = p["decision"]
        if decision not in ("release", "uphold", "reassign"):
            raise ValidationError("争议结论只能是 release / uphold / reassign")
        order = self.orders[d["order_id"]]
        events: list[tuple[str, dict]] = [
            (
                EV_DISPUTE_RESOLVED,
                {
                    "dispute_id": p["dispute_id"],
                    "order_id": order["order_id"],
                    "decision": decision,
                    "note": p.get("note", ""),
                    "new_company_id": p.get("new_company_id"),
                },
            )
        ]
        if decision == "release":
            if order["state"] in FROZEN_LIKE:
                events.append(
                    (
                        EV_SUBSIDY_RELEASED,
                        {
                            "order_id": order["order_id"],
                            "pool_id": order["pool_id"],
                            "activity_id": order["activity_id"],
                            "amount_cents": order["subsidy_cents"],
                            "reason": f"审计解锁争议 {p['dispute_id']}：解除冻结",
                        },
                    )
                )
            elif order["state"] == ST_SETTLED:
                settled_in = order.get("settlement_id")
                sealed = bool(
                    settled_in and self.settlements[settled_in].get("snapshot_id")
                )
                if not sealed:
                    events.append(
                        (
                            EV_SUBSIDY_RECOVERED,
                            {
                                "order_id": order["order_id"],
                                "pool_id": order["pool_id"],
                                "activity_id": order["activity_id"],
                                "amount_cents": order["subsidy_cents"],
                                "settlement_id": settled_in,
                                "reason": f"审计解锁争议 {p['dispute_id']}：封账前追回",
                            },
                        )
                    )
                else:
                    events.append(
                        (
                            EV_POST_CLOSE_ADJUSTMENT,
                            {
                                "adjustment_id": f"ADJ-{p['dispute_id']}",
                                "type": ADJ_DISPUTE_RELEASE,
                                "order_id": order["order_id"],
                                "company_id": order["company_id"],
                                "activity_id": order["activity_id"],
                                "pool_id": order["pool_id"],
                                "period_id": order.get("period_id"),
                                "snapshot_id": self.settlements[settled_in]["snapshot_id"],
                                "amount_cents": order["subsidy_cents"],
                                "reason": f"审计解锁争议 {p['dispute_id']}：补贴回池，下期追减",
                                "status": "pending",
                            },
                        )
                    )
        elif decision == "reassign":
            if not p.get("new_company_id"):
                raise ValidationError("reassign 结论需要 new_company_id")
            if p["new_company_id"] not in self.companies:
                raise ReferenceError(f"企业不存在：{p['new_company_id']}")
            if p["new_company_id"] == order["company_id"]:
                raise ValidationError("改派对象不能是当前承担企业")
            events.append(
                (
                    EV_POST_CLOSE_ADJUSTMENT,
                    {
                        "adjustment_id": f"ADJ-{p['dispute_id']}",
                        "type": ADJ_DISPUTE_REASSIGN,
                        "order_id": order["order_id"],
                        "company_id": order["company_id"],
                        "new_company_id": p["new_company_id"],
                        "activity_id": order["activity_id"],
                        "pool_id": order["pool_id"],
                        "period_id": order.get("period_id"),
                        "snapshot_id": d.get("snapshot_id"),
                        "amount_cents": order["subsidy_cents"],
                        "reason": f"审计解锁争议 {p['dispute_id']}：返还责任改派至 "
                                  f"{p['new_company_id']}",
                        "status": "pending",
                    },
                )
            )
        return events

    # ==================================================================
    # 事件投影
    # ==================================================================

    def apply(self, event: Any) -> None:
        t = event.type
        p = event.payload
        self.head_seq = event.seq
        self.head_digest = event.digest

        if t == EV_COMPANY_REGISTERED:
            self.companies[p["company_id"]] = {
                "company_id": p["company_id"], "name": p["name"]
            }

        elif t == EV_BUDGET_DEPOSITED:
            pool = self.pools.setdefault(
                p["pool_id"],
                {"pool_id": p["pool_id"], "total_cents": 0, "frozen_cents": 0,
                 "settled_cents": 0, "recovered_cents": 0},
            )
            pool["total_cents"] = p["total_after_cents"]

        elif t == EV_ACTIVITY_DEFINED:
            self.activities[p["activity_id"]] = dict(p)

        elif t == EV_BATCH_DEFINED:
            self.batches[p["batch_id"]] = dict(p)

        elif t == EV_WINDOW_OPENED:
            self.periods[p["period_id"]] = {
                "period_id": p["period_id"],
                "activity_ids": list(p["activity_ids"]),
                "status": "open",
                "settlement_id": None,
                "opened_at": p["opened_at"],
            }

        elif t == EV_REPORT_RECEIVED:
            self.reports.append(dict(p, seq=event.seq))
            self._index_channel(p, event.seq, p.get("report_id", ""))

        elif t == EV_REDEMPTION_RECEIVED:
            order = self._get_or_create_order(p["order_id"], p["company_id"])
            order["activity_id"] = p["activity_id"]
            order["pool_id"] = p["pool_id"]
            order["gross_cents"] = p["gross_cents"]
            order["subsidy_cents"] = p["subsidy_cents"]
            order["period_id"] = p.get("period_id") or order.get("period_id")
            order["redemption"] = {
                "batch_id": p["batch_id"],
                "code": p["code"],
                "seq": event.seq,
                "rule_event_seq": p["rule_event_seq"],
                "channel_ref": p["channel_ref"],
            }
            self.code_index[p["code"]] = p["batch_id"]
            self._index_channel(p, event.seq, p["order_id"])
            payment_ref = p.get("payment_reference") or (
                order.get("payment", {}).get("payment_reference")
                if order.get("payment") else None
            )
            if payment_ref and not self.consumed.get(payment_ref):
                self._consume(payment_ref, p["activity_id"], p["order_id"])

        elif t == EV_SUBSIDY_FROZEN:
            order = self.orders[p["order_id"]]
            if not order.get("frozen_at_seq"):
                order["frozen_at_seq"] = event.seq
                self.pools[p["pool_id"]]["frozen_cents"] += p["amount_cents"]
            order["state"] = ST_FROZEN if order.get("payment") else ST_PENDING
            self._drain_pending_refunds(order)

        elif t == EV_PAYMENT_RECEIVED:
            order = self._get_or_create_order(p["order_id"], p["company_id"])
            if not order.get("payment"):
                order["payment"] = {
                    "payment_reference": p["payment_reference"],
                    "amount_cents": p["amount_cents"],
                    "seq": event.seq,
                    "channel_ref": p["channel_ref"],
                }
                if order.get("activity_id"):
                    self._consume(
                        p["payment_reference"], order["activity_id"], p["order_id"]
                    )
            if p.get("period_id"):
                order["period_id"] = p["period_id"]
            self._index_channel(p, event.seq, p["order_id"])
            if order.get("redemption") and order["state"] == ST_PENDING:
                order["state"] = ST_FROZEN
            self._drain_pending_refunds(order)

        elif t == EV_REFUND_RECEIVED:
            self._index_channel(p, event.seq, p["order_id"])
            if p["effect"] == "parked":
                self.pending_refunds[p["order_id"]].append(
                    {"payload": p, "seq": event.seq}
                )
            else:
                self.orders[p["order_id"]].setdefault("refunds", []).append(
                    {
                        "refund_id": p["refund_id"],
                        "seq": event.seq,
                        "amount_cents": p["amount_cents"],
                        "full_refund": p.get("full_refund", False),
                        "cross_company": p.get("cross_company", False),
                        "responsible_company_id": p.get("responsible_company_id"),
                        "effect": p["effect"],
                        "adjustment_id": p.get("adjustment_id"),
                    }
                )

        elif t == EV_SUBSIDY_RELEASED:
            order = self.orders[p["order_id"]]
            pool = self.pools[p["pool_id"]]
            pool["frozen_cents"] -= p["amount_cents"]
            order["state"] = (
                ST_RELEASED if "争议" in p["reason"] else ST_REFUNDED
            )
            order["release_seq"] = event.seq

        elif t == EV_SUBSIDY_RECOVERED:
            order = self.orders[p["order_id"]]
            pool = self.pools[p["pool_id"]]
            pool["settled_cents"] -= p["amount_cents"]
            pool["recovered_cents"] += p["amount_cents"]
            order["state"] = ST_CLAWED_BACK
            order["clawback_seq"] = event.seq
            settlement = self.settlements.get(p.get("settlement_id", ""))
            if settlement is not None and not settlement.get("snapshot_id"):
                self._correct_unsealed_settlement(settlement, order, p["amount_cents"], event.seq)

        elif t == EV_POST_CLOSE_ADJUSTMENT:
            adj = dict(p)
            adj["seq"] = event.seq
            self.adjustments.append(adj)
            if p["type"] in (ADJ_CLAWBACK, ADJ_DISPUTE_RELEASE):
                order = self.orders[p["order_id"]]
                pool = self.pools[p["pool_id"]]
                pool["settled_cents"] -= p["amount_cents"]
                pool["recovered_cents"] += p["amount_cents"]
                order["state"] = ST_CLAWED_BACK
                order["clawback_seq"] = event.seq

        elif t == EV_WINDOW_SETTLED:
            moved: dict[str, int] = defaultdict(int)
            for e in p["entries"]:
                moved[e["pool_id"]] += e["subsidy_cents"]
                order = self.orders[e["order_id"]]
                order["state"] = ST_SETTLED
                order["settlement_id"] = p["settlement_id"]
                order["settled_seq"] = event.seq
            for pid, amount in moved.items():
                pool = self.pools[pid]
                pool["frozen_cents"] -= amount
                pool["settled_cents"] += amount
            for adj_id in p["applied_adjustments"]:
                for adj in self.adjustments:
                    if adj["adjustment_id"] == adj_id and adj["status"] == "pending":
                        adj["status"] = "applied"
                        adj["applied_settlement_id"] = p["settlement_id"]
                        adj["applied_seq"] = event.seq
            self.settlements[p["settlement_id"]] = {k: v for k, v in p.items()}
            self.periods[p["period_id"]]["status"] = "settled"
            self.periods[p["period_id"]]["settlement_id"] = p["settlement_id"]

        elif t == EV_BOOKS_CLOSED:
            snapshot = p["snapshot"]
            self.snapshots[snapshot["snapshot_id"]] = snapshot
            self.periods[p["period_id"]]["status"] = "closed"
            self.settlements[p["settlement_id"]]["snapshot_id"] = snapshot["snapshot_id"]

        elif t == EV_DISPUTE_RAISED:
            self.disputes[p["dispute_id"]] = {
                "dispute_id": p["dispute_id"],
                "order_id": p["order_id"],
                "reason": p["reason"],
                "status": "open",
                "raised_seq": event.seq,
                "snapshot_id": p.get("snapshot_id"),
            }
            self.orders[p["order_id"]]["dispute_id"] = p["dispute_id"]

        elif t == EV_DISPUTE_RESOLVED:
            d = self.disputes[p["dispute_id"]]
            d["status"] = "resolved"
            d["decision"] = p["decision"]
            d["resolved_seq"] = event.seq
            d["note"] = p.get("note", "")
            self.orders[p["order_id"]].pop("dispute_id", None)

        elif t == EV_LATE_ARRIVAL_QUARANTINED:
            self.quarantine.append(dict(p, seq=event.seq))

        else:
            raise SettlementError(f"重放遇到未知事件类型：{t}")

        if event.key and event.key not in self.idem:
            self.idem[event.key] = {
                "seq": event.seq,
                "type": event.type,
                "fingerprint": p.get("command_fingerprint", fingerprint(p)),
            }

    # ------------------------------------------------------------------
    # 投影辅助
    # ------------------------------------------------------------------

    def _index_channel(self, p: dict, seq: int, order_id: str) -> None:
        ref = p.get("channel_ref")
        if ref:
            self.channel_index[(p.get("source", "unknown"), ref)] = {
                "order_id": order_id,
                "fingerprint": p.get("fingerprint", fingerprint(p)),
                "seq": seq,
            }

    def _get_or_create_order(self, order_id: str, company_id: str) -> dict:
        return self.orders.setdefault(
            order_id,
            {"order_id": order_id, "company_id": company_id, "state": ST_PENDING},
        )

    def _consume(self, payment_reference: str, activity_id: str, order_id: str) -> None:
        self.consumed.setdefault(
            payment_reference, {"activity_id": activity_id, "order_id": order_id}
        )

    def _drain_pending_refunds(self, order: dict) -> None:
        """核销/支付补齐后，处理先于订单到达的挂起退款：直接释放冻结。

        只有订单已经完成冻结，挂起退款才能生效；否则继续保留，等冻结
        事件到来时再处理，避免退款被静默吞掉。
        """
        if not order.get("frozen_at_seq"):
            return
        parked = self.pending_refunds.pop(order["order_id"], [])
        if not parked:
            return
        released = False
        for item in parked:
            p = item["payload"]
            order.setdefault("refunds", []).append(
                {
                    "refund_id": p["refund_id"],
                    "seq": item["seq"],
                    "amount_cents": p["amount_cents"],
                    "full_refund": True,
                    "cross_company": False,
                    "responsible_company_id": order["company_id"],
                    "effect": "released_from_parked",
                    "adjustment_id": None,
                }
            )
            if not released and order.get("frozen_at_seq"):
                self.pools[order["pool_id"]]["frozen_cents"] -= order["subsidy_cents"]
                order["state"] = ST_REFUNDED
                order["release_seq"] = item["seq"]
                released = True

    def _correct_unsealed_settlement(
        self, settlement: dict, order: dict, amount: int, seq: int
    ) -> None:
        """封账前追回：同步更正尚未封账的结算单，保证封账定格即最终数。"""
        kept = []
        hit = False
        for e in settlement["entries"]:
            if e["order_id"] == order["order_id"] and not hit:
                hit = True
                continue
            kept.append(e)
        settlement["entries"] = kept
        settlement.setdefault("clawbacks", []).append(
            {"order_id": order["order_id"], "amount_cents": amount, "seq": seq}
        )
        cid = order["company_id"]
        tc = settlement["totals_company"].get(cid)
        if tc:
            tc["gross_cents"] -= order["gross_cents"]
            tc["subsidy_cents"] -= amount
            tc["orders"] -= 1
        ta = settlement["totals_activity"].get(order["activity_id"])
        if ta:
            ta["subsidy_cents"] -= amount
            ta["orders"] -= 1

    def _is_closed(self, period_id: str) -> bool:
        period = self.periods.get(period_id)
        return bool(period and period["status"] == "closed")

    def _calc_subsidy(self, activity: dict, batch: dict, gross_cents: int) -> int:
        if batch.get("face_value_cents"):
            amount = int(batch["face_value_cents"])
        else:
            amount = gross_cents * activity["ratio_permille"] // 1000
        cap = activity.get("per_order_cap_cents")
        if cap is not None:
            amount = min(amount, int(cap))
        return min(amount, gross_cents)

    def _quarantine(
        self, command_type: str, key: str, p: dict, reason: str
    ) -> tuple[str, dict]:
        return (
            EV_LATE_ARRIVAL_QUARANTINED,
            {
                "command_type": command_type,
                "key": key,
                "reason": reason,
                "payload": dict(p),
                "fingerprint": fingerprint(p),
                "at": _now(),
            },
        )

    # ==================================================================
    # 对账与追溯
    # ==================================================================

    def pool_view(self, pool_id: str) -> dict:
        if pool_id not in self.pools:
            raise ReferenceError(f"预算池不存在：{pool_id}")
        pool = self.pools[pool_id]
        return {
            "pool_id": pool_id,
            "total_cents": pool["total_cents"],
            "frozen_cents": pool["frozen_cents"],
            "settled_cents": pool["settled_cents"],
            "available_cents": available_of(pool),
            "recovered_cents": pool["recovered_cents"],
            "identity_total_check": pool["total_cents"]
            == available_of(pool) + pool["frozen_cents"] + pool["settled_cents"],
        }

    def reconcile(self) -> dict:
        """全局对账：池恒等式 + 池余额与订单汇总逐项勾稽。"""
        frozen_by_pool: dict[str, int] = defaultdict(int)
        settled_by_pool: dict[str, int] = defaultdict(int)
        for order in self.orders.values():
            if not order.get("pool_id"):
                continue
            if order["state"] in (ST_FROZEN, ST_PENDING):
                frozen_by_pool[order["pool_id"]] += order["subsidy_cents"]
            elif order["state"] == ST_SETTLED:
                settled_by_pool[order["pool_id"]] += order["subsidy_cents"]
        problems = []
        for pid, pool in self.pools.items():
            if pool["frozen_cents"] != frozen_by_pool.get(pid, 0):
                problems.append(
                    f"池 {pid} 冻结 {pool['frozen_cents']} 与订单汇总 "
                    f"{frozen_by_pool.get(pid, 0)} 不符"
                )
            if pool["settled_cents"] != settled_by_pool.get(pid, 0):
                problems.append(
                    f"池 {pid} 已结算 {pool['settled_cents']} 与订单汇总 "
                    f"{settled_by_pool.get(pid, 0)} 不符"
                )
            if pool["total_cents"] != available_of(pool) + pool["frozen_cents"] + pool["settled_cents"]:
                problems.append(f"池 {pid} 总额恒等式不成立")
        return {
            "ok": not problems,
            "problems": problems,
            "pools": {pid: self.pool_view(pid) for pid in sorted(self.pools)},
            "orders_by_state": {
                s: sum(1 for o in self.orders.values() if o.get("state") == s)
                for s in (ST_PENDING, ST_FROZEN, ST_SETTLED, ST_REFUNDED,
                          ST_CLAWED_BACK, ST_RELEASED)
            },
            "quarantined": len(self.quarantine),
            "pending_adjustments": sum(
                1 for a in self.adjustments if a["status"] == "pending"
            ),
        }

    def trace_order(self, order_id: str) -> dict:
        order = self.orders.get(order_id)
        if order is None:
            raise ReferenceError(f"订单不存在：{order_id}")
        steps = []
        if order.get("redemption"):
            steps.append(("redemption 核销", order["redemption"]["seq"]))
        pay = order.get("payment")
        if pay:
            steps.append(("payment 银联支付", pay["seq"]))
        if order.get("frozen_at_seq"):
            steps.append(("额度冻结", order["frozen_at_seq"]))
        if order.get("settled_seq"):
            steps.append(("窗口分账", order["settled_seq"]))
        for rf in order.get("refunds", []):
            steps.append((f"退款 {rf['refund_id']}（{rf['effect']}）", rf["seq"]))
        if order.get("release_seq"):
            steps.append(("冻结释放/争议放行", order["release_seq"]))
        if order.get("clawback_seq"):
            steps.append(("补贴追回", order["clawback_seq"]))
        snapshot_id = None
        if order.get("settlement_id"):
            snapshot_id = self.settlements[order["settlement_id"]].get("snapshot_id")
        return {
            "order": {k: v for k, v in order.items()},
            "timeline": [
                {"step": name, "event_seq": seq}
                for name, seq in sorted(steps, key=lambda x: x[1])
            ],
            "settlement_id": order.get("settlement_id"),
            "snapshot_id": snapshot_id,
            "dispute_id": order.get("dispute_id"),
            "adjustments": [a for a in self.adjustments if a.get("order_id") == order_id],
        }

    def trace_settlement(self, settlement_id: str) -> dict:
        """从一张结算单的汇总数字逐级钻取到每笔原始核销与支付事件。"""
        doc = self.settlements.get(settlement_id)
        if doc is None:
            raise ReferenceError(f"结算单不存在：{settlement_id}")
        entries = []
        for e in doc["entries"]:
            order = self.orders[e["order_id"]]
            entries.append(
                {
                    "entry": e,
                    "redemption_event_seq": order["redemption"]["seq"],
                    "payment_event_seq": order["payment"]["seq"],
                    "refunds": order.get("refunds", []),
                }
            )
        return {
            "settlement": doc,
            "snapshot": self.snapshots.get(doc.get("snapshot_id")),
            "entries_with_provenance": entries,
            "drilldown": (
                f"窗口 {doc['period_id']} 分账 {len(doc['entries'])} 笔 → "
                f"{len(doc['totals_company'])} 家企业；每笔分录带 "
                f"redemption_event_seq / payment_event_seq，可直接回查原始事件"
            ),
        }
