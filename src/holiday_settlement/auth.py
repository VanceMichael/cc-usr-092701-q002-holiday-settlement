"""角色与权限：财务管金额、主办方管规则、审计管争议。"""

from __future__ import annotations

from dataclasses import dataclass
from .errors import AuthorizationError

# 四种业务角色
OPERATOR = "operator"    # 商圈运营人员：接收记录、查看、发起争议
FINANCE = "finance"      # 财务人员：只处理金额（预算/对账/封账/分账）
ORGANIZER = "organizer"  # 活动主办方：维护规则
AUDITOR = "auditor"      # 审计角色：解锁争议单

ROLES = (OPERATOR, FINANCE, ORGANIZER, AUDITOR)

# 命令类型 -> 允许的角色集合
PERMISSIONS: dict[str, tuple[str, ...]] = {
    # 商圈运营人员接收企业与各类流水记录
    "register_company": (OPERATOR,),
    # 活动主办方维护规则
    "define_activity": (ORGANIZER,),
    "define_coupon_batch": (ORGANIZER,),
    # 商圈运营人员接收各类记录
    "receive_redemption": (OPERATOR,),
    "receive_refund": (OPERATOR,),
    "receive_payment": (OPERATOR,),
    # 财务人员处理金额
    "deposit_budget": (FINANCE,),
    "open_window": (OPERATOR, FINANCE),
    "settle_window": (FINANCE,),
    "close_books": (FINANCE,),
    # 发起争议可由运营/财务/审计提出，只有审计能解锁
    "raise_dispute": (OPERATOR, FINANCE, AUDITOR),
    "resolve_dispute": (AUDITOR,),
}


@dataclass(frozen=True)
class Principal:
    """当前操作人。"""

    name: str
    role: str

    def __post_init__(self) -> None:
        if self.role not in ROLES:
            raise ValueError(f"未知角色：{self.role}")


def require_role(command_type: str, principal: Principal) -> None:
    allowed = PERMISSIONS[command_type]
    if principal.role not in allowed:
        raise AuthorizationError(
            f"角色 {principal.role} 无权执行 {command_type}，允许：{','.join(allowed)}"
        )
