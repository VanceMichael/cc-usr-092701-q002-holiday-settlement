"""结算协同服务门面：鉴权 -> 命令校验 -> 事件追加 -> 状态投影。

服务绑定一个哈希链 Journal（可落盘）。重启时自动重放全部事件重建状态，
因此额度、退款与已结算金额在乱序回执、进程重启后仍然相互吻合。
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from .auth import Principal, require_role
from .errors import SettlementError
from .journal import Journal
from .records import to_yuan
from .state import SettlementState, fingerprint


class Receipt:
    """一次命令的处理结果。"""

    def __init__(self, key: str, events: list, duplicated: bool = False) -> None:
        self.key = key
        self.events = events
        self.duplicated = duplicated

    @property
    def event_seqs(self) -> list[int]:
        return [e.seq for e in self.events]

    @property
    def quarantined(self) -> bool:
        return any(e.type == "late_arrival_quarantined" for e in self.events)

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "duplicated": self.duplicated,
            "event_seqs": self.event_seqs,
            "quarantined": self.quarantined,
            "events": [
                {"seq": e.seq, "type": e.type, "payload": e.payload}
                for e in self.events
            ],
        }


class SettlementService:
    def __init__(self, journal_path: str | Path | None = None) -> None:
        self.journal = Journal(journal_path)
        self.state = SettlementState()
        self.journal.replay(self.state.apply)

    # ==================================================================
    # 命令执行
    # ==================================================================

    def execute(
        self,
        command_type: str,
        key: str,
        payload: dict,
        principal: Principal,
    ) -> Receipt:
        """执行一条命令。

        - 先鉴权（财务/主办方/运营/审计各有权限边界）；
        - 状态机校验并产出事件；
        - 事件按序追加哈希链并投影；追加与投影之间不允许其他写入
          （单服务实例内串行处理）。
        """
        require_role(command_type, principal)
        planned = self.state.handle(command_type, key, payload)
        if not planned:
            # 同内容重传：幂等吸收，不产生新事件
            return Receipt(key, [], duplicated=True)
        produced = []
        for event_type, event_payload in planned:
            event = self.journal.append(
                event_id=f"EV-{uuid.uuid4().hex[:16]}",
                event_type=event_type,
                key=key,
                payload=event_payload,
                actor=principal.name,
            )
            self.state.apply(event)
            produced.append(event)
        return Receipt(key, produced)

    def ingest(
        self, commands: list[dict], principal: Principal
    ) -> list[Receipt]:
        """批量接收分批到达的记录，按给出的顺序串行排空。

        每条命令形如 ``{"type", "key", "payload"}``；单条失败不影响
        其余记录，失败与隔离都会在结果清单中显式标注。
        """
        receipts: list[Receipt] = []
        for cmd in commands:
            try:
                receipts.append(
                    self.execute(cmd["type"], cmd["key"], cmd["payload"], principal)
                )
            except SettlementError as exc:
                receipts.append(
                    _FailedReceipt(cmd.get("key", ""), type(exc).__name__, str(exc))
                )
        return receipts

    # ==================================================================
    # 查询 / 对账 / 追溯
    # ==================================================================

    def reconcile(self) -> dict:
        return self.state.reconcile()

    def pool_view(self, pool_id: str) -> dict:
        return self.state.pool_view(pool_id)

    def trace_order(self, order_id: str) -> dict:
        return self.state.trace_order(order_id)

    def trace_settlement(self, settlement_id: str) -> dict:
        return self.state.trace_settlement(settlement_id)

    def settlement(self, settlement_id: str) -> dict:
        return self.state.trace_settlement(settlement_id)

    def snapshot(self, snapshot_id: str) -> dict:
        from .errors import ReferenceError

        if snapshot_id not in self.state.snapshots:
            raise ReferenceError(f"封账快照不存在：{snapshot_id}")
        return self.state.snapshots[snapshot_id]

    def quarantine_list(self) -> list[dict]:
        return list(self.state.quarantine)

    def adjustments(self, status: str | None = None) -> list[dict]:
        if status is None:
            return list(self.state.adjustments)
        return [a for a in self.state.adjustments if a["status"] == status]

    def disputes(self) -> dict[str, dict]:
        return dict(self.state.disputes)

    @property
    def head_seq(self) -> int:
        return self.state.head_seq

    @property
    def head_digest(self) -> str:
        return self.state.head_digest or self.journal.head_digest

    def explain_entry(self, settlement_id: str, order_id: str) -> dict[str, Any]:
        """返回单笔分账的完整解释：规则版本、券码、支付、事件链。"""
        doc = self.state.trace_settlement(settlement_id)
        for item in doc["entries_with_provenance"]:
            if item["entry"]["order_id"] == order_id:
                return item
        raise SettlementError(f"结算单 {settlement_id} 中没有订单 {order_id}")

    @staticmethod
    def yuan(cents: int) -> str:
        return to_yuan(cents)


class _FailedReceipt(Receipt):
    def __init__(self, key: str, error_type: str, message: str) -> None:
        super().__init__(key, [])
        self.error_type = error_type
        self.error_message = message

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "error": self.error_type,
            "message": self.error_message,
        }
