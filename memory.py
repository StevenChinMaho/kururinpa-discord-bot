"""聊天的長期記憶與頻道摘要（SQLite）。資料量很小，直接同步存取即可。"""

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime

SCHEMA = """
CREATE TABLE IF NOT EXISTS memories (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id   INTEGER NOT NULL,          -- 私訊為 0
    user_id    INTEGER,                   -- NULL 表示伺服器共同記憶
    user_name  TEXT,
    content    TEXT NOT NULL,
    created_by INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_memories_scope ON memories (guild_id, user_id);

CREATE TABLE IF NOT EXISTS channel_summaries (
    channel_id      INTEGER PRIMARY KEY,
    summary         TEXT NOT NULL,
    last_message_id INTEGER NOT NULL,     -- 摘要已涵蓋到哪則訊息（含）
    updated_at      TEXT NOT NULL
);
"""


@dataclass
class Memory:
    id: int
    user_id: int | None
    user_name: str | None
    content: str
    created_by: int

    def fmt(self) -> str:
        return f"#{self.id} {self.content}"


class MemoryStore:
    def __init__(self, path: str):
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._db.executescript(SCHEMA)

    def _query(self, sql: str, params: tuple = ()) -> list[tuple]:
        with self._lock:
            return self._db.execute(sql, params).fetchall()

    def _write(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._lock, self._db:
            return self._db.execute(sql, params)

    @staticmethod
    def _rows(rows: list[tuple]) -> list[Memory]:
        return [Memory(*r) for r in rows]

    # ---------- 記憶 ----------

    def add(self, guild_id: int, user_id: int | None, user_name: str | None, content: str, created_by: int) -> int:
        cur = self._write(
            "INSERT INTO memories (guild_id, user_id, user_name, content, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (guild_id, user_id, user_name, content, created_by, datetime.now().isoformat(timespec="seconds")),
        )
        return cur.lastrowid

    def count(self, guild_id: int, user_id: int | None) -> int:
        return self._query(
            "SELECT COUNT(*) FROM memories WHERE guild_id = ? AND user_id IS ?", (guild_id, user_id)
        )[0][0]

    def list_for(self, guild_id: int, user_id: int | None, limit: int = 50) -> list[Memory]:
        """user_id=None 時列出伺服器共同記憶。"""
        return self._rows(self._query(
            "SELECT id, user_id, user_name, content, created_by FROM memories "
            "WHERE guild_id = ? AND user_id IS ? ORDER BY id DESC LIMIT ?",
            (guild_id, user_id, limit),
        ))[::-1]

    def search(self, guild_id: int, keywords: list[str], limit: int = 20) -> list[Memory]:
        """在整個伺服器（含所有人的個人記憶）找包含任一關鍵字的記憶。"""
        keywords = [k for k in keywords if k] or [""]
        where = " OR ".join("content LIKE ? OR user_name LIKE ?" for _ in keywords)
        params = [p for k in keywords for p in (f"%{k}%", f"%{k}%")]
        return self._rows(self._query(
            f"SELECT id, user_id, user_name, content, created_by FROM memories "
            f"WHERE guild_id = ? AND ({where}) ORDER BY id DESC LIMIT ?",
            (guild_id, *params, limit),
        ))

    def get(self, guild_id: int, memory_id: int) -> Memory | None:
        rows = self._rows(self._query(
            "SELECT id, user_id, user_name, content, created_by FROM memories WHERE guild_id = ? AND id = ?",
            (guild_id, memory_id),
        ))
        return rows[0] if rows else None

    def delete(self, guild_id: int, memory_id: int) -> bool:
        return self._write("DELETE FROM memories WHERE guild_id = ? AND id = ?", (guild_id, memory_id)).rowcount > 0

    # ---------- 頻道摘要 ----------

    def get_summary(self, channel_id: int) -> tuple[str, int] | None:
        rows = self._query("SELECT summary, last_message_id FROM channel_summaries WHERE channel_id = ?", (channel_id,))
        return rows[0] if rows else None

    def set_summary(self, channel_id: int, summary: str, last_message_id: int):
        self._write(
            "INSERT INTO channel_summaries (channel_id, summary, last_message_id, updated_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT (channel_id) DO UPDATE SET summary = excluded.summary, "
            "last_message_id = excluded.last_message_id, updated_at = excluded.updated_at",
            (channel_id, summary, last_message_id, datetime.now().isoformat(timespec="seconds")),
        )

    def clear_summary(self, channel_id: int) -> bool:
        return self._write("DELETE FROM channel_summaries WHERE channel_id = ?", (channel_id,)).rowcount > 0
