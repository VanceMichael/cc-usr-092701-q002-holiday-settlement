"""命令与事件记录：所有进出场数据都是显式、可序列化的记录。

金额一律以整数「分」在领域内部传递；命令入口接受元（Decimal/字符串/数字），
由 ``to_cents`` 换算，避免浮点误差。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any

# ---------------------------------------------------------------------------
# 金额
# ---------------------------------------------------------------------------


def to_cents(value: int | float | str | Decimal) -> int:
    """元转分，四舍五入到整分。"""
    if isinstance(value, int):
        cents = value
    else:
        cents = int((Decimal(str(value)) * 100).quantize(Decimal("1")))
    if cents < 0:
        raise ValueError("金额不能为负")
    return cents


def to_yuan(cents: int) -> str:
    """分转元的展示字符串。"""
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return f"{sign}{cents // 100}.{cents % 100:02d}"


# ---------------------------------------------------------------------------
# 命令（外部请求）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Command:
    type: str
    key: str  # 幂等键：渠道流水号/命令编号
    payload: dict[str, Any]
    actor: str = ""
    role: str = ""


# ---------------------------------------------------------------------------
# 事件（日志记录）。seq/hash 由 Journal 填写，这里只声明业务负载。
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Event:
    seq: int
    event_id: str
    type: str
    key: str  # 触发该事件的命令幂等键（便于追溯）
    payload: dict[str, Any]
    actor: str
    prev_hash: str = ""
    digest: str = ""

    def to_line(self) -> dict[str, Any]:
        return asdict(self)


# 事件类型常量
EV_COMPANY_REGISTERED = "company_registered"
EV_ACTIVITY_DEFINED = "activity_defined"
EV_BATCH_DEFINED = "batch_defined"
EV_BUDGET_DEPOSITED = "budget_deposited"
EV_REDEMPTION_RECEIVED = "redemption_received"
EV_PAYMENT_RECEIVED = "payment_received"
EV_REFUND_RECEIVED = "refund_received"
EV_SUBSIDY_FROZEN = "subsidy_frozen"
EV_SUBSIDY_RELEASED = "subsidy_released"
EV_WINDOW_SETTLED = "window_settled"
EV_BOOKS_CLOSED = "books_closed"
EV_DISPUTE_RAISED = "dispute_raised"
EV_DISPUTE_RESOLVED = "dispute_resolved"
EV_SUBSIDY_RECOVERED = "subsidy_recovered"
EV_LATE_ARRIVAL_QUARANTINED = "late_arrival_quarantined"
EV_POST_CLOSE_ADJUSTMENT = "post_close_adjustment"
EV_REPORT_RECEIVED = "report_received"

# 命令类型 -> 成功时产生的事件类型
COMMAND_EVENTS: dict[str, str] = {
    "register_company": EV_COMPANY_REGISTERED,
    "define_activity": EV_ACTIVITY_DEFINED,
    "define_coupon_batch": EV_BATCH_DEFINED,
    "deposit_budget": EV_BUDGET_DEPOSITED,
    "receive_redemption": EV_REDEMPTION_RECEIVED,
    "receive_payment": EV_PAYMENT_RECEIVED,
    "receive_refund": EV_REFUND_RECEIVED,
    "receive_report": EV_REPORT_RECEIVED,
    "raise_dispute": EV_DISPUTE_RAISED,
}
