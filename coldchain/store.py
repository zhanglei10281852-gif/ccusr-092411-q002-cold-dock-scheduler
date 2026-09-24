"""SQLite 事件库。

事件只追加、不修改、不删除。每个聚合维护单调序号，提交时用
``expected_seq`` 做乐观并发控制：两个调度员同时操作同一预约时，
只有一个事务成功，另一个收到 :class:`StaleVersion` 后必须重新加载再试。
跨聚合的原子追加（如两个调度员同时接受候补）在单事务内完成，
容量上限的最终裁决由应用层在事务内重算保证。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from .events import Event

SCHEMA = """
create table if not exists event_log (
    event_id       text primary key,
    event_type     text not null,
    aggregate_id   text not null,
    aggregate_type text not null,
    seq            integer not null,
    occurred_at    text not null,
    payload        text not null,
    unique (aggregate_id, seq)
);
create index if not exists idx_event_log_occurred on event_log(occurred_at);
create table if not exists resource_config (
    resource_type text not null,
    resource_id   text not null,
    zone          text not null,
    capacity      integer not null,
    primary key (resource_type, resource_id)
);
"""


class DuplicateEvent(Exception):
    """事件 ID 或 (聚合, 序号) 已存在。"""


class StaleVersion(Exception):
    """乐观并发失败：期望序号与库内现状不一致。"""

    def __init__(self, aggregate_id: str, expected: int, actual: int) -> None:
        super().__init__(
            f"聚合 {aggregate_id} 序号冲突：期望 {expected}，实际 {actual}"
        )
        self.aggregate_id = aggregate_id
        self.expected = expected
        self.actual = actual


class EventStore:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self._conn = sqlite3.connect(
            self.path, timeout=30, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("pragma journal_mode=wal")
        self._conn.execute("pragma foreign_keys=on")
        self._conn.execute("pragma busy_timeout=30000")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "EventStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- 读取 -----------------------------------------------------------

    def load_events(self, aggregate_id: str | None = None) -> list[Event]:
        # 重放顺序 = 提交顺序（rowid 单调递增），不能按 occurred_at 排序：
        # 事后补录的命令（如 11:00 录入 07:00 的设备停用）发生时间更早，
        # 按发生时间重放会颠倒跨聚合的因果关系。
        if aggregate_id is None:
            rows = self._conn.execute(
                "select * from event_log order by rowid"
            ).fetchall()
        else:
            rows = self._conn.execute(
                "select * from event_log where aggregate_id = ? order by seq",
                (aggregate_id,),
            ).fetchall()
        from .events import Event as _Event
        import json

        return [
            _Event(
                event_id=row["event_id"],
                event_type=row["event_type"],
                aggregate_id=row["aggregate_id"],
                aggregate_type=row["aggregate_type"],
                seq=row["seq"],
                occurred_at=row["occurred_at"],
                payload=json.loads(row["payload"]),
            )
            for row in rows
        ]

    def last_seq(self, aggregate_id: str) -> int:
        row = self._conn.execute(
            "select coalesce(max(seq), 0) as s from event_log where aggregate_id = ?",
            (aggregate_id,),
        ).fetchone()
        return int(row["s"])

    def exists_event(self, event_id: str) -> bool:
        return self._conn.execute(
            "select 1 from event_log where event_id = ?", (event_id,)
        ).fetchone() is not None

    # -- 写入 -----------------------------------------------------------

    def append(
        self,
        events: list[Event],
        expected_versions: dict[str, int] | None = None,
    ) -> None:
        """原子追加一批事件。

        ``expected_versions`` 把聚合 ID 映射到调用方读取时的最后序号；
        库内序号更大即并发冲突，整批回滚。
        """
        if not events:
            return
        import json

        conn = self._conn
        conn.execute("begin immediate")
        try:
            for aggregate_id, expected in (expected_versions or {}).items():
                actual = self.last_seq(aggregate_id)
                if actual != expected:
                    raise StaleVersion(aggregate_id, expected, actual)
            for event in events:
                current = self.last_seq(event.aggregate_id)
                if event.seq != current + 1:
                    raise StaleVersion(
                        event.aggregate_id, event.seq - 1, current
                    )
                conn.execute(
                    "insert into event_log values (?, ?, ?, ?, ?, ?, ?)",
                    (
                        event.event_id,
                        event.event_type,
                        event.aggregate_id,
                        event.aggregate_type,
                        event.seq,
                        event.occurred_at,
                        json.dumps(
                            event.payload,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                )
        except Exception:
            conn.execute("rollback")
            raise
        conn.execute("commit")

    # -- 资源配置 -------------------------------------------------------

    def save_resources(self, resources: list[tuple[str, str, str, int]]) -> None:
        self._conn.execute("begin immediate")
        try:
            self._conn.execute("delete from resource_config")
            self._conn.executemany(
                "insert into resource_config values (?, ?, ?, ?)",
                resources,
            )
        except Exception:
            self._conn.execute("rollback")
            raise
        self._conn.execute("commit")

    def load_resources(self) -> list[tuple[str, str, str, int]]:
        rows = self._conn.execute(
            "select resource_type, resource_id, zone, capacity "
            "from resource_config order by resource_type, resource_id"
        ).fetchall()
        return [tuple(row) for row in rows]
