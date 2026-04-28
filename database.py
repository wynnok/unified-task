import sqlite3
import json
import logging
import os
import shutil
import threading
from contextlib import contextmanager
from datetime import datetime
from calendar import monthrange
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo


def current_time_text(timezone_name: Optional[str] = None) -> str:
    timezone = ZoneInfo(timezone_name or os.environ.get("APP_TIMEZONE", "Asia/Shanghai"))
    return datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S")


class Database:
    def __init__(self, db_path: str, auto_migrate: bool = True):
        self.db_path = db_path
        self.local = threading.local()
        if auto_migrate:
            self._init_db()

    @staticmethod
    def _configure_connection(conn: sqlite3.Connection) -> sqlite3.Connection:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @staticmethod
    def _require_existing_db_path(db_path: str) -> str:
        resolved_path = os.path.abspath(db_path)
        if not os.path.isfile(resolved_path):
            raise FileNotFoundError(f"Database file does not exist: {resolved_path}")
        return resolved_path

    @classmethod
    def summarize_db_path(cls, db_path: str) -> Dict[str, Any]:
        db = cls(cls._require_existing_db_path(db_path), auto_migrate=False)
        try:
            return db.summarize_counts()
        finally:
            db.close()

    @classmethod
    def upgrade_prod_db_in_place(cls, db_path: str) -> Dict[str, Any]:
        target_path = cls._require_existing_db_path(db_path)
        temp_upgrade_path = f"{target_path}.upgrade_tmp"

        if os.path.exists(temp_upgrade_path):
            raise FileExistsError(f"Temporary upgrade file already exists: {temp_upgrade_path}")

        try:
            cls.prepare_prod_db_upgrade(target_path, temp_upgrade_path)
            os.replace(temp_upgrade_path, target_path)
            return cls.summarize_db_path(target_path)
        finally:
            if os.path.exists(temp_upgrade_path):
                os.remove(temp_upgrade_path)

    @classmethod
    def prepare_prod_db_upgrade(cls, source_db_path: str, upgraded_db_path: str) -> Dict[str, Any]:
        source_path = cls._require_existing_db_path(source_db_path)
        upgraded_path = os.path.abspath(upgraded_db_path)

        if source_path == upgraded_path:
            raise ValueError("source_db_path and upgraded_db_path must be different")
        if os.path.exists(upgraded_path):
            raise FileExistsError(f"Upgrade destination already exists: {upgraded_path}")

        shutil.copy2(source_path, upgraded_path)
        try:
            db = cls(upgraded_path)
            try:
                return db.summarize_counts()
            finally:
                db.close()
        except Exception:
            if os.path.exists(upgraded_path):
                os.remove(upgraded_path)
            raise

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self.local, "conn"):
            self.local.conn = self._configure_connection(
                sqlite3.connect(self.db_path, check_same_thread=False)
            )
        return self.local.conn

    def close(self) -> None:
        conn = getattr(self.local, "conn", None)
        if conn is not None:
            conn.close()
            del self.local.conn

    @contextmanager
    def transaction(self):
        conn = self._get_conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def _init_db(self):
        with self.transaction() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sort_order INTEGER NOT NULL,
                    name TEXT NOT NULL UNIQUE,
                    icon TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    message TEXT,
                    url TEXT,
                    cron_expression TEXT NOT NULL,
                    channel TEXT NOT NULL CHECK(channel IN ('email', 'webhook')),
                    enabled BOOLEAN NOT NULL DEFAULT 1,
                    tags TEXT,
                    group_id INTEGER REFERENCES groups(id),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS execution_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('success', 'failed')),
                    error TEXT,
                    executed_at TEXT NOT NULL,
                    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    user TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    last_activity TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_groups_sort_order ON groups(sort_order, id);
                CREATE INDEX IF NOT EXISTS idx_execution_history_task_id ON execution_history(task_id);
                CREATE INDEX IF NOT EXISTS idx_execution_history_executed_at ON execution_history(executed_at);
                CREATE INDEX IF NOT EXISTS idx_sessions_last_activity ON sessions(last_activity);
            """)

        self._ensure_task_group_column()
        default_group = self.ensure_default_group()
        self.backfill_task_groups(default_group["id"])
        self._ensure_task_group_foreign_key()
        self._ensure_task_group_index()

    def _table_exists(self, table_name: str) -> bool:
        conn = self._get_conn()
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        )
        return cursor.fetchone() is not None

    def _table_columns(self, table_name: str) -> set:
        if not self._table_exists(table_name):
            return set()
        conn = self._get_conn()
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        return {row["name"] for row in cursor.fetchall()}

    def _count_rows(self, table_name: str) -> int:
        if not self._table_exists(table_name):
            return 0
        conn = self._get_conn()
        cursor = conn.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}")
        return int(cursor.fetchone()["cnt"])

    def _index_names(self, table_name: str) -> set:
        if not self._table_exists(table_name):
            return set()
        conn = self._get_conn()
        cursor = conn.execute(f"PRAGMA index_list({table_name})")
        return {row["name"] for row in cursor.fetchall()}

    def summarize_counts(self) -> Dict[str, Any]:
        task_columns = self._table_columns("tasks")
        conn = self._get_conn()
        default_group = None

        if self._table_exists("groups"):
            cursor = conn.execute("SELECT * FROM groups WHERE name = ? LIMIT 1", ("默认",))
            row = cursor.fetchone()
            default_group = dict(row) if row else None

        null_group_ids = 0
        invalid_group_ids = 0
        if "group_id" in task_columns:
            cursor = conn.execute(
                """
                SELECT COUNT(*) AS cnt FROM tasks
                WHERE group_id IS NULL OR group_id = ''
                """
            )
            null_group_ids = int(cursor.fetchone()["cnt"])

            if self._table_exists("groups"):
                cursor = conn.execute(
                    """
                    SELECT COUNT(*) AS cnt FROM tasks
                    WHERE group_id IS NOT NULL
                      AND group_id != ''
                      AND group_id NOT IN (SELECT id FROM groups)
                    """
                )
                invalid_group_ids = int(cursor.fetchone()["cnt"])

        return {
            "tasks": self._count_rows("tasks"),
            "execution_history": self._count_rows("execution_history"),
            "sessions": self._count_rows("sessions"),
            "settings": self._count_rows("settings"),
            "groups": self._count_rows("groups"),
            "default_group_name": default_group["name"] if default_group else None,
            "default_group_icon": default_group["icon"] if default_group else None,
            "null_group_ids": null_group_ids,
            "invalid_group_ids": invalid_group_ids,
            "tasks_missing_group_id_column": "group_id" not in task_columns,
            "indexes": {
                "tasks": self._index_names("tasks"),
                "groups": self._index_names("groups"),
                "execution_history": self._index_names("execution_history"),
                "sessions": self._index_names("sessions"),
            },
        }

    def _ensure_task_group_column(self) -> None:
        if "group_id" in self._table_columns("tasks"):
            return

        with self.transaction() as conn:
            conn.execute("ALTER TABLE tasks ADD COLUMN group_id INTEGER")

    def _task_group_has_foreign_key(self) -> bool:
        if not self._table_exists("tasks"):
            return False
        conn = self._get_conn()
        cursor = conn.execute("PRAGMA foreign_key_list(tasks)")
        return any(row[2] == "groups" and row[3] == "group_id" and row[4] == "id" for row in cursor.fetchall())

    def _ensure_task_group_foreign_key(self) -> None:
        if self._task_group_has_foreign_key():
            return

        with self.transaction() as conn:
            conn.execute("PRAGMA foreign_keys = OFF")
            try:
                conn.execute(
                    """
                    CREATE TABLE tasks__migrated (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT NOT NULL,
                        message TEXT,
                        url TEXT,
                        cron_expression TEXT NOT NULL,
                        channel TEXT NOT NULL CHECK(channel IN ('email', 'webhook')),
                        enabled BOOLEAN NOT NULL DEFAULT 1,
                        tags TEXT,
                        group_id INTEGER REFERENCES groups(id),
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO tasks__migrated (id, title, message, url, cron_expression, channel, enabled, tags, group_id, created_at, updated_at)
                    SELECT id, title, message, url, cron_expression, channel, enabled, tags, group_id, created_at, updated_at
                    FROM tasks
                    """
                )
                conn.execute("DROP TABLE tasks")
                conn.execute("ALTER TABLE tasks__migrated RENAME TO tasks")
            except Exception:
                conn.execute("DROP TABLE IF EXISTS tasks__migrated")
                raise
            finally:
                conn.execute("PRAGMA foreign_keys = ON")

    def _ensure_task_group_index(self) -> None:
        with self.transaction() as conn:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_group_id ON tasks(group_id)")

    def get_all_groups(self) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        cursor = conn.execute("SELECT * FROM groups ORDER BY sort_order ASC, id ASC")
        return [dict(row) for row in cursor.fetchall()]

    def get_group_by_id(self, group_id: int) -> Optional[Dict[str, Any]]:
        conn = self._get_conn()
        cursor = conn.execute("SELECT * FROM groups WHERE id = ?", (group_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def create_group(self, group: Dict[str, Any]) -> int:
        now = current_time_text()
        with self.transaction() as conn:
            cursor = conn.execute("""
                INSERT INTO groups (sort_order, name, icon, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                group["sort_order"],
                group["name"],
                group["icon"],
                now,
                now,
            ))
            return cursor.lastrowid

    def update_group(self, group_id: int, group: Dict[str, Any]) -> bool:
        now = current_time_text()
        with self.transaction() as conn:
            cursor = conn.execute("""
                UPDATE groups
                SET sort_order = ?, name = ?, icon = ?, updated_at = ?
                WHERE id = ?
            """, (
                group["sort_order"],
                group["name"],
                group["icon"],
                now,
                group_id,
            ))
            return cursor.rowcount > 0

    def delete_group(self, group_id: int) -> bool:
        if self.count_tasks_by_group(group_id) > 0:
            return False

        with self.transaction() as conn:
            cursor = conn.execute("DELETE FROM groups WHERE id = ?", (group_id,))
            return cursor.rowcount > 0

    def count_tasks_by_group(self, group_id: int) -> int:
        conn = self._get_conn()
        cursor = conn.execute("SELECT COUNT(*) AS cnt FROM tasks WHERE group_id = ?", (group_id,))
        return int(cursor.fetchone()["cnt"])

    def ensure_default_group(self) -> Dict[str, Any]:
        conn = self._get_conn()
        cursor = conn.execute("SELECT * FROM groups WHERE name = ? LIMIT 1", ("默认",))
        default_row = cursor.fetchone()

        cursor = conn.execute("SELECT * FROM groups WHERE name = ? LIMIT 1", ("默认分组",))
        legacy_row = cursor.fetchone()

        if default_row:
            if legacy_row:
                now = current_time_text()
                with self.transaction() as tx_conn:
                    tx_conn.execute(
                        "UPDATE tasks SET group_id = ? WHERE group_id = ?",
                        (default_row["id"], legacy_row["id"]),
                    )
                    tx_conn.execute(
                        "DELETE FROM groups WHERE id = ?",
                        (legacy_row["id"],),
                    )
                    tx_conn.execute(
                        "UPDATE groups SET updated_at = ? WHERE id = ?",
                        (now, default_row["id"]),
                    )
            return self.get_group_by_id(default_row["id"])

        if legacy_row:
            now = current_time_text()
            with self.transaction() as tx_conn:
                tx_conn.execute(
                    "UPDATE groups SET name = ?, updated_at = ? WHERE id = ?",
                    ("默认", now, legacy_row["id"]),
                )
            return self.get_group_by_id(legacy_row["id"])

        group_id = self.create_group({"sort_order": 1, "name": "默认", "icon": "folder"})
        return self.get_group_by_id(group_id)

    def backfill_task_groups(self, default_group_id: int):
        if "group_id" not in self._table_columns("tasks"):
            return

        with self.transaction() as conn:
            conn.execute("""
                UPDATE tasks
                SET group_id = ?
                WHERE group_id IS NULL
                   OR group_id = ''
                   OR group_id NOT IN (SELECT id FROM groups)
            """, (default_group_id,))

    def get_all_tasks(self) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        cursor = conn.execute("""
            SELECT t.*,
                   g.name as group_name,
                   g.icon as group_icon,
                   eh.status as last_status,
                   eh.error as last_error,
                   eh.executed_at as last_run_at
            FROM tasks t
            LEFT JOIN groups g ON t.group_id = g.id
            LEFT JOIN (
                SELECT task_id, status, error, executed_at,
                       ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY executed_at DESC) as rn
                FROM execution_history
            ) eh ON t.id = eh.task_id AND eh.rn = 1
            ORDER BY t.id DESC
        """)
        return [dict(row) for row in cursor.fetchall()]

    def get_task_by_id(self, task_id: int) -> Optional[Dict[str, Any]]:
        conn = self._get_conn()
        cursor = conn.execute("""
            SELECT t.*,
                   g.name as group_name,
                   g.icon as group_icon,
                   eh.status as last_status,
                   eh.error as last_error,
                   eh.executed_at as last_run_at
            FROM tasks t
            LEFT JOIN groups g ON t.group_id = g.id
            LEFT JOIN (
                SELECT task_id, status, error, executed_at,
                       ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY executed_at DESC) as rn
                FROM execution_history
            ) eh ON t.id = eh.task_id AND eh.rn = 1
            WHERE t.id = ?
        """, (task_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def _normalize_task_group_id(self, group_id: Any, default_group_id: int) -> int:
        try:
            normalized_group_id = int(group_id)
        except (TypeError, ValueError):
            normalized_group_id = default_group_id

        if not self.get_group_by_id(normalized_group_id):
            normalized_group_id = default_group_id

        return normalized_group_id

    def create_task(self, task: Dict[str, Any]) -> int:
        now = current_time_text()
        default_group_id = self.ensure_default_group()["id"]
        group_id = self._normalize_task_group_id(task.get("group_id", default_group_id), default_group_id)

        with self.transaction() as conn:
            cursor = conn.execute("""
                INSERT INTO tasks (title, message, url, cron_expression, channel, enabled, tags, group_id, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                task["title"],
                task.get("message", ""),
                task.get("url", ""),
                task["cron_expression"],
                task["channel"],
                task.get("enabled", True),
                json.dumps(task.get("tags", [])),
                group_id,
                now,
                now
            ))
            return cursor.lastrowid

    def update_task(self, task_id: int, task: Dict[str, Any]) -> bool:
        now = current_time_text()
        default_group_id = self.ensure_default_group()["id"]
        group_id = self._normalize_task_group_id(task.get("group_id", default_group_id), default_group_id)

        with self.transaction() as conn:
            cursor = conn.execute("""
                UPDATE tasks
                SET title = ?, message = ?, url = ?, cron_expression = ?,
                    channel = ?, enabled = ?, tags = ?, group_id = ?, updated_at = ?
                WHERE id = ?
            """, (
                task["title"],
                task.get("message", ""),
                task.get("url", ""),
                task["cron_expression"],
                task["channel"],
                task.get("enabled", True),
                json.dumps(task.get("tags", [])),
                group_id,
                now,
                task_id
            ))
            return cursor.rowcount > 0

    def delete_task(self, task_id: int) -> bool:
        with self.transaction() as conn:
            cursor = conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            return cursor.rowcount > 0

    def batch_update_enabled(self, task_ids: List[int], enabled: bool) -> int:
        now = current_time_text()
        with self.transaction() as conn:
            placeholders = ",".join("?" * len(task_ids))
            cursor = conn.execute(f"""
                UPDATE tasks SET enabled = ?, updated_at = ? WHERE id IN ({placeholders})
            """, [enabled, now] + task_ids)
            return cursor.rowcount

    def batch_delete_tasks(self, task_ids: List[int]) -> int:
        with self.transaction() as conn:
            placeholders = ",".join("?" * len(task_ids))
            cursor = conn.execute(f"DELETE FROM tasks WHERE id IN ({placeholders})", task_ids)
            return cursor.rowcount

    def add_execution_record(self, task_id: int, status: str, error: Optional[str] = None):
        now = current_time_text()
        with self.transaction() as conn:
            conn.execute("""
                INSERT INTO execution_history (task_id, status, error, executed_at)
                VALUES (?, ?, ?, ?)
            """, (task_id, status, error, now))

    def get_execution_history(self, task_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        cursor = conn.execute("""
            SELECT * FROM execution_history
            WHERE task_id = ?
            ORDER BY executed_at DESC
            LIMIT ?
        """, (task_id, limit))
        return [dict(row) for row in cursor.fetchall()]

    def get_month_execution_count(
        self,
        task_id: int,
        when: Optional[datetime] = None,
        timezone_name: str = "Asia/Shanghai",
    ) -> int:
        timezone = ZoneInfo(timezone_name)
        reference = when or datetime.now(timezone)
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone)
        else:
            reference = reference.astimezone(timezone)

        month_start = reference.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_day = monthrange(reference.year, reference.month)[1]
        month_end = reference.replace(day=last_day, hour=23, minute=59, second=59, microsecond=0)
        conn = self._get_conn()
        cursor = conn.execute(
            """
            SELECT COUNT(*) AS cnt FROM execution_history
            WHERE task_id = ? AND executed_at >= ? AND executed_at <= ?
            """,
            (
                task_id,
                month_start.strftime("%Y-%m-%d %H:%M:%S"),
                month_end.strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        return int(cursor.fetchone()["cnt"])

    def get_statistics(self, days: int = 7) -> Dict[str, Any]:
        conn = self._get_conn()
        cursor = conn.execute("""
            SELECT
                COUNT(CASE WHEN status = 'success' THEN 1 END) as success_count,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_count,
                COUNT(*) as total_executions
            FROM execution_history
            WHERE executed_at >= datetime('now', '-' || ? || ' days')
        """, (days,))
        return dict(cursor.fetchone())

    def create_session(self, session_id: str, user: str):
        now = current_time_text()
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO sessions (session_id, user, created_at, last_activity)
                VALUES (?, ?, ?, ?)
            """, (session_id, user, now, now))

    def update_session_activity(self, session_id: str):
        now = current_time_text()
        with self.transaction() as conn:
            conn.execute("""
                UPDATE sessions SET last_activity = ? WHERE session_id = ?
            """, (now, session_id))

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        conn = self._get_conn()
        cursor = conn.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def delete_expired_sessions(self, timeout_minutes: int):
        with self.transaction() as conn:
            conn.execute("""
                DELETE FROM sessions
                WHERE last_activity < datetime('now', '-' || ? || ' minutes')
            """, (timeout_minutes,))

    def export_tasks(self) -> List[Dict[str, Any]]:
        tasks = self.get_all_tasks()
        for task in tasks:
            if task.get("tags"):
                task["tags"] = json.loads(task["tags"])
            task.pop("last_status", None)
            task.pop("last_error", None)
            task.pop("last_run_at", None)
            task.pop("group_name", None)
            task.pop("group_icon", None)
        return tasks

    def import_tasks(self, tasks: List[Dict[str, Any]]) -> int:
        count = 0
        default_group_id = self.ensure_default_group()["id"]
        for task in tasks:
            try:
                imported_task = dict(task)
                group_id = imported_task.get("group_id")
                try:
                    normalized_group_id = int(group_id)
                except (TypeError, ValueError):
                    normalized_group_id = default_group_id

                if not self.get_group_by_id(normalized_group_id):
                    normalized_group_id = default_group_id

                imported_task["group_id"] = normalized_group_id
                self.create_task(imported_task)
                count += 1
            except Exception as e:
                logging.error(f"Failed to import task: {e}")
        return count

    def get_setting(self, key: str) -> Optional[str]:
        conn = self._get_conn()
        cursor = conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cursor.fetchone()
        return row["value"] if row else None

    def set_setting(self, key: str, value: str):
        now = current_time_text()
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO settings (key, value, updated_at)
                VALUES (?, ?, ?)
            """, (key, value, now))

    def get_all_settings(self) -> Dict[str, str]:
        conn = self._get_conn()
        cursor = conn.execute("SELECT key, value FROM settings")
        return {row["key"]: row["value"] for row in cursor.fetchall()}

    def init_default_settings(self, settings: Dict[str, Any]):
        """Initialize settings from dict structure"""
        now = current_time_text()
        with self.transaction() as conn:
            # Check if settings already exist
            cursor = conn.execute("SELECT COUNT(*) as cnt FROM settings")
            if cursor.fetchone()["cnt"] > 0:
                return  # Already initialized

            # Flatten nested dict to key-value pairs
            for section, values in settings.items():
                if isinstance(values, dict):
                    for key, value in values.items():
                        full_key = f"{section}.{key}"
                        conn.execute("""
                            INSERT INTO settings (key, value, updated_at)
                            VALUES (?, ?, ?)
                        """, (full_key, json.dumps(value), now))
