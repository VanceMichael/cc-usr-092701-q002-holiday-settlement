"""结算协同服务的领域异常。"""

from __future__ import annotations


class SettlementError(Exception):
    """所有业务校验失败的基类。"""


class AuthorizationError(SettlementError):
    """当前角色无权执行该命令。"""


class ValidationError(SettlementError):
    """命令字段缺失或取值非法。"""


class ClosedPeriodError(SettlementError):
    """结算窗口已封账，历史补报不能改写快照。"""


class DuplicateCommandError(SettlementError):
    """同一个幂等键重复送达（内容不同则拒绝，内容相同直接返回原结果）。"""


class BudgetExhaustedError(SettlementError):
    """可用额度不足：活动预算或共享年度预算已冻结/结算完毕。"""


class ReferenceError(SettlementError):
    """命令引用了不存在的企业/活动/批次/订单/快照。"""


class OrderStateError(SettlementError):
    """订单当前状态不允许该动作（例如尚未支付匹配就申请分账）。"""


class DisputeStateError(SettlementError):
    """争议单当前状态不允许该动作，或操作人不是审计角色。"""


class RuleConflictError(SettlementError):
    """同一消费尝试在两个活动里各领一次补贴。"""
