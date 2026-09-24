"""SQLite 事件库。

单写者 + 乐观版本号：每个命令基于读取时的 ``expected_version`` 提交，
两个调度员同时接受候补时必有一方收到 :class:`ConflictError``，重试时基于
最新状态重新排程，因此任何资源上限都不会被并发突破。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

from .events import ALL_EVENT_TYPES, Event

SCHEMA = """
create table if not exists events (
    seq          integer primary key autoincrement,
    event_id     text unique not null,
    event_type   text not null,
    aggregate_id text not null,
    occurred_at  integer not null,
    payload      text not null
);
create index if not exists events_agg on events(aggregate_id, seq);
create table if not exists idempotency (
    idem_key  text primary key,
    event_ids text not null
);
"""


class ConflictError(RuntimeError):
    """提交时事件日志版本已落后（并发修改）。"""


class EventStore:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self._conn = sqlite3.connect(self.path, isolation_level=None)
        self._conn.execute("pragma journal_mode=WAL")
        self._conn.execute("pragma foreign_keys=ON")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def load(self) -> list[Event]:
        rows = self._conn.execute(
            "select seq, event_id, event_type, aggregate_id, occurred_at, "
            "payload from events order by seq").fetchall()
        return [Event.from_row(*row) for row in rows]

    def current_version(self) -> int:
        row = self._conn.execute("select coalesce(max(seq),0) from events") \
            .fetchone()
        return int(row[0])

    def lookup_idempotency(self, idem_key: str) -> list[Event] | None:
        """幂等键已存在则返回当时提交的事件，否则 None。"""
        row = self._conn.execute(
            "select event_ids from idempotency where idem_key=?",
            (idem_key,)).fetchone()
        if row is None:
            return None
        ids = json.loads(row[0])
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        rows = self._conn.execute(
            f"select seq, event_id, event_type, aggregate_id, occurred_at, "
            f"payload from events where event_id in ({placeholders}) "
            f"order by seq", ids).fetchall()
        return [Event.from_row(*r) for r in rows]

    def append(self, events: list[Event], expected_version: int,
               idem_key: str | None = None) -> list[Event]:
        """原子提交一批事件。

        - ``expected_version`` 必须等于库中当前最大序号，否则 ConflictError；
        - 相同 ``idem_key`` 重放直接返回已提交事件，不重复落库；
        - event_id 由内容与序号确定性派生，重放同一命令得到同一标识。
        """
        if not events and idem_key is None:
            return []
        conn = self._conn
        conn.execute("begin immediate")
        try:
            version = conn.execute(
                "select coalesce(max(seq),0) from events").fetchone()[0]
            if version != expected_version:
                raise ConflictError(
                    f"版本冲突：期望 {expected_version}，实际 {version}")

            stored: list[Event] = []
            next_seq = version
            for event in events:
                if event.event_type not in ALL_EVENT_TYPES:
                    raise ValueError(f"未知事件类型：{event.event_type}")
                next_seq += 1
                payload_raw = json.dumps(event.payload, ensure_ascii=False,
                                         sort_keys=True)
                digest = hashlib.sha1(
                    f"{event.event_type}|{event.aggregate_id}|"
                    f"{event.occurred_at}|{next_seq}|{payload_raw}"
                    .encode("utf-8")).hexdigest()
                event_id = f"evt-{digest[:16]}"
                conn.execute(
                    "insert into events(event_id, event_type, aggregate_id, "
                    "occurred_at, payload) values (?, ?, ?, ?, ?)",
                    (event_id, event.event_type, event.aggregate_id,
                     event.occurred_at, payload_raw))
                stored.append(Event(
                    seq=next_seq, event_id=event_id,
                    event_type=event.event_type,
                    aggregate_id=event.aggregate_id,
                    occurred_at=event.occurred_at, payload=event.payload))

            if idem_key is not None:
                conn.execute(
                    "insert into idempotency(idem_key, event_ids) "
                    "values (?, ?)",
                    (idem_key, json.dumps([e.event_id for e in stored])))
            conn.execute("commit")
            return stored
        except Exception:
            conn.execute("rollback")
            raise

    def events_for(self, aggregate_id: str) -> list[Event]:
        rows = self._conn.execute(
            "select seq, event_id, event_type, aggregate_id, occurred_at, "
            "payload from events where aggregate_id=? order by seq",
            (aggregate_id,)).fetchall()
        return [Event.from_row(*row) for row in rows]
