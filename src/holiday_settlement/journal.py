"""哈希链事件日志：服务重启后重放，状态与日志严格一致。

- 每条事件包含前一条事件的 SHA-256 摘要，任何事后篡改都会在加载时暴露。
- 命令幂等键 -> 事件序号的索引由调用方（Service）维护在状态里；
  日志本身只负责顺序追加、落盘、重放、完整性校验。
- 落盘采用 JSON Lines（每行一个事件），原子替换（临时文件 + os.replace）。
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Callable, Iterable

from .records import Event

GENESIS = "0" * 64


def digest_for(event: Event) -> str:
    body = json.dumps(
        {
            "seq": event.seq,
            "event_id": event.event_id,
            "type": event.type,
            "key": event.key,
            "payload": event.payload,
            "actor": event.actor,
            "prev_hash": event.prev_hash,
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


class Journal:
    """内存追加日志，可选绑定一个 JSONL 文件做持久化。"""

    def __init__(self, path: str | Path | None = None) -> None:
        self._events: list[Event] = []
        self._path = Path(path) if path else None
        if self._path is not None and self._path.exists():
            self._load()

    # ------------------------------------------------------------------
    # 追加
    # ------------------------------------------------------------------

    def append(
        self,
        event_id: str,
        event_type: str,
        key: str,
        payload: dict,
        actor: str,
    ) -> Event:
        seq = len(self._events) + 1
        prev_hash = self._events[-1].digest if self._events else GENESIS
        event = Event(
            seq=seq,
            event_id=event_id,
            type=event_type,
            key=key,
            payload=payload,
            actor=actor,
            prev_hash=prev_hash,
        )
        object.__setattr__(event, "digest", digest_for(event))
        self._events.append(event)
        if self._path is not None:
            self._flush()
        return event

    # ------------------------------------------------------------------
    # 读取 / 重放
    # ------------------------------------------------------------------

    @property
    def events(self) -> list[Event]:
        return list(self._events)

    @property
    def head_digest(self) -> str:
        return self._events[-1].digest if self._events else GENESIS

    def replay(self, handler: Callable[[Event], None]) -> None:
        """按顺序把事件交给状态投影处理。"""
        for event in self._events:
            handler(event)

    # ------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------

    def _flush(self) -> None:
        assert self._path is not None
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self._path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                for event in self._events:
                    fh.write(json.dumps(event.to_line(), ensure_ascii=False) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self._path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def _load(self) -> None:
        assert self._path is not None
        raw_events: list[Event] = []
        with self._path.open("r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                event = Event(**data)
                expected_prev = raw_events[-1].digest if raw_events else GENESIS
                if event.prev_hash != expected_prev:
                    raise ValueError(
                        f"事件日志第 {line_no} 行链接断裂：前序摘要不匹配"
                    )
                if digest_for(event) != event.digest:
                    raise ValueError(
                        f"事件日志第 {line_no} 行摘要校验失败，记录可能被篡改"
                    )
                if event.seq != line_no:
                    raise ValueError(
                        f"事件日志第 {line_no} 行序号不连续"
                    )
                raw_events.append(event)
        self._events = raw_events
