"""节日商圈结算协同的领域模型。

只包含纯数据与纯函数：金额一律使用 ``Decimal`` 并按分规整，订单内容通过
规范化 JSON 计算 SHA-256 指纹，用于把"金额相同但内容不同"的渠道重传与
真正的重复投递区分开。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Any


CENT = Decimal("0.00")


def money(value: str | int | Decimal) -> Decimal:
    """把外部输入统一成两位小数的金额，拒绝负数。"""
    amount = Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)
    if amount < 0:
        raise ValueError("金额不能为负")
    return amount


def rate(value: str | Decimal) -> Decimal:
    """补贴比例，限定在 [0, 1]。"""
    ratio = Decimal(str(value))
    if not 0 <= ratio <= 1:
        raise ValueError("补贴比例必须在 0 到 1 之间")
    return ratio


def canonical_json(value: Any) -> str:
    """排序键、无空白的 JSON 文本，作为指纹与哈希输入。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_default)


def _default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, (date,)):
        return obj.isoformat()
    if isinstance(obj, Enum):
        return obj.value
    raise TypeError(f"不可序列化的类型：{type(obj)!r}")


def content_fingerprint(payload: Any) -> str:
    """订单/记录内容指纹：金额相同但内容不同 => 指纹不同。"""
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


class Role(str, Enum):
    """三类业务角色：财务只管金额、主办方维护规则、审计解锁争议。"""

    FINANCE = "finance"        # 财务人员：封账、分账、对账等金额动作
    ORGANIZER = "organizer"    # 活动主办方：维护活动规则、预算、券批次
    AUDITOR = "auditor"        # 审计角色：解锁争议单、查看全链路溯源


@dataclass(frozen=True)
class User:
    name: str
    roles: tuple[Role, ...] = ()

    def can(self, role: Role) -> bool:
        return role in self.roles


# ---------------------------------------------------------------- 配置指令

@dataclass(frozen=True)
class BudgetSpec:
    """年度预算池：可被多个活动共享。"""

    budget_id: str
    total: Decimal


@dataclass(frozen=True)
class CampaignSpec:
    """活动补贴规则。

    - 共享年度预算时填 ``budget_id``；独立额度填 ``standalone_budget``。
    - ``discount_rate`` 补贴比例，``max_subsidy_per_order`` 单笔封顶。
    - ``order_dedup_field`` 指定消费去重字段（默认支付单号），跨活动
      同一笔消费只能享受一次补贴。
    """

    campaign_id: str
    name: str
    discount_rate: Decimal
    settlement_day: date
    budget_id: str | None = None
    standalone_budget: Decimal | None = None
    max_subsidy_per_order: Decimal | None = None
    order_dedup_field: str = "payment_id"
    merchants: frozenset[str] = frozenset()


@dataclass(frozen=True)
class BatchSpec:
    """券批次：归属活动，含券码清单与本批补贴额度上限。"""

    batch_id: str
    campaign_id: str
    codes: frozenset[str]
    subsidy_quota: Decimal


# ---------------------------------------------------------------- 往来记录

@dataclass(frozen=True)
class PaymentData:
    """银联/渠道支付对账记录。"""

    record_id: str
    payment_id: str
    merchant_id: str
    amount: Decimal
    occurred_at: date
    source: str = "unionpay"


@dataclass(frozen=True)
class RedemptionData:
    """券码核销记录（品牌快闪、夜游、餐饮节等渠道分批送达）。"""

    record_id: str
    redemption_id: str
    campaign_id: str
    merchant_id: str
    payment_id: str
    coupon_code: str
    amount: Decimal
    order_content: dict[str, Any]
    occurred_at: date


@dataclass(frozen=True)
class RefundData:
    """退款记录（可能跨店发起、可能早于核销/支付回执到达）。"""

    record_id: str
    refund_id: str
    payment_id: str
    amount: Decimal
    merchant_id: str
    occurred_at: date


# ---------------------------------------------------------------- 运行态实体

@dataclass
class AnnualBudget:
    total: Decimal
    frozen: Decimal = Decimal("0.00")
    settled: Decimal = Decimal("0.00")
    recovered: Decimal = Decimal("0.00")          # 封账后退回，计入回收而非额度
    late_settled: Decimal = Decimal("0.00")      # 封账后补报形成的调整金额

    @property
    def available(self) -> None:
        raise AttributeError("available 由服务结合独立额度计算，避免双池口径混淆")


@dataclass
class BatchState:
    """券批次运行态：本批补贴额度的冻结/结算/后期/冲回。"""

    spec: BatchSpec
    frozen: Decimal = Decimal("0.00")
    settled: Decimal = Decimal("0.00")
    late_settled: Decimal = Decimal("0.00")
    recovered: Decimal = Decimal("0.00")
    used_codes: set[str] = field(default_factory=set)

    @property
    def available(self) -> Decimal:
        return self.spec.subsidy_quota - self.frozen - self.settled - self.late_settled


@dataclass
class Campaign:
    spec: CampaignSpec
    budget_total: Decimal
    budget_kind: str                 # "shared" | "standalone"
    frozen: Decimal = Decimal("0.00")
    settled: Decimal = Decimal("0.00")
    recovered: Decimal = Decimal("0.00")
    late_settled: Decimal = Decimal("0.00")
    sealed_snapshot_id: str | None = None

    @property
    def sealed(self) -> bool:
        return self.sealed_snapshot_id is not None

    @property
    def available_budget(self) -> Decimal:
        """可冻结额度：封账后的回收不重新释放为可用预算。"""
        return self.budget_total - self.frozen - self.settled - self.late_settled
