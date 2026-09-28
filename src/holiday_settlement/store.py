"""仅追加的事件日志与不可变快照存储。

所有状态变化都先写成一条带哈希链的事件，再重放进内存；这样服务重启或
收到乱序回执后，可以从同一份事实重建出完全一致的额度、退款与已结算金额。

- ``events.log.jsonl``：追加写，每行一个事件，事件含 ``prev_hash`` 形成
  哈希链，任何历史事件被改写都会在 :meth:`EventStore.verify_chain` 暴露。
- ``snapshots/<id>.json``：封账快照一次性写入、永不覆盖（含生成时末事件
  哈希，与事件链绑定）。补报数据只能产生新的"后期调整"，无法改写快照。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from .model import canonical_json, content_fingerprint


def to_jsonable(value: Any) -> Any:
    """把领域对象转成可进 JSON 的纯类型（Decimal 保留为字符串避免浮点误差）。"""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_jsonable(v) for v in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


@dataclass(frozen=True)
class Event:
    seq: int
    kind: str
    actor: str
    payload: dict[str, Any]
    prev_hash: str
    digest: str

    def to_line(self) -> str:
        body = {
            "seq": self.seq,
            "kind": self.kind,
            "actor": self.actor,
            "payload": to_jsonable(self.payload),
            "prev_hash": self.prev_hash,
        }
        body["digest"] = content_fingerprint(body)
        return canonical_json(body)

    @staticmethod
    def from_line(line: str) -> "Event":
        raw = json.loads(line)
        digest = raw.pop("digest")
        if content_fingerprint(raw) != digest:
            raise ValueError(f"事件 {raw.get('seq')} 哈希不匹配，日志可能被篡改")
        return Event(
            seq=raw["seq"], kind=raw["kind"], actor=raw["actor"],
            payload=raw["payload"], prev_hash=raw["prev_hash"], digest=digest,
        )


class EventStore:
    """文件型事件存储；内存态服务通过重放这些事件构建。"""

    def __init__(self, directory: str | Path):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        (self.dir / "snapshots").mkdir(exist_ok=True)
        self.log_path = self.dir / "events.log.jsonl"
        self._seq = 0
        self._tail = "GENESIS"
        if self.log_path.exists():
            for event in self._read_raw():
                self._seq = event.seq
                self._tail = event.digest

    # ------------------------------------------------------------ 读写事件

    def append(self, kind: str, actor: str, payload: dict[str, Any]) -> Event:
        event = Event(
            seq=self._seq + 1, kind=kind, actor=actor,
            payload=payload, prev_hash=self._tail, digest="",
        )
        line = event.to_line()
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        stored = Event.from_line(line)
        self._seq = stored.seq
        self._tail = stored.digest
        return stored

    def read_events(self) -> list[Event]:
        return list(self._read_raw()) if self.log_path.exists() else []

    def _read_raw(self) -> Iterable[Event]:
        with self.log_path.open("r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, 1):
                line = line.strip()
                if line:
                    yield Event.from_line(line)

    def verify_chain(self) -> bool:
        """校验全部事件的哈希链与序号连续性；任何篡改都返回 False。"""
        prev = "GENESIS"
        if not self.log_path.exists():
            return True
        with self.log_path.open("r", encoding="utf-8") as fh:
            for expected_seq, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    return False
                try:
                    raw = json.loads(line)
                    digest = raw.pop("digest")
                except (json.JSONDecodeError, KeyError):
                    return False
                if content_fingerprint(raw) != digest:
                    return False
                if raw.get("seq") != expected_seq or raw.get("prev_hash") != prev:
                    return False
                prev = digest
        return True

    # ------------------------------------------------------------ 封账快照

    def snapshot_path(self, snapshot_id: str) -> Path:
        return self.dir / "snapshots" / f"{snapshot_id}.json"

    def write_snapshot(self, snapshot_id: str, snapshot: dict[str, Any]) -> Path:
        """一次性写入封账快照；已存在则拒绝——快照不可改写。"""
        path = self.snapshot_path(snapshot_id)
        if path.exists():
            raise PermissionError(f"封账快照 {snapshot_id} 已存在，禁止改写")
        doc = {"snapshot_id": snapshot_id, **to_jsonable(snapshot)}
        doc["snapshot_fingerprint"] = content_fingerprint(
            {k: v for k, v in doc.items()}
        )
        path.write_text(canonical_json(doc) + "\n", encoding="utf-8")
        return path

    def read_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        return json.loads(self.snapshot_path(snapshot_id).read_text(encoding="utf-8"))

    def list_snapshots(self) -> list[str]:
        return sorted(p.stem for p in (self.dir / "snapshots").glob("*.json"))
