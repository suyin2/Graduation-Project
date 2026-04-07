from typing import Any, Dict, List, Optional

from database.db import get_connection


class UpdateLogRepository:
    """
    更新日志仓库层：
    - 记录某次主题更新
    - 查询某主题最近一次更新
    - 查询某主题历史更新记录
    """

    def insert_log(
        self,
        topic: str,
        source: str = "arxiv",
        query_used: str = "",
        fetched_count: int = 0,
        inserted_count: int = 0,
        skipped_count: int = 0,
    ) -> int:
        if not topic:
            raise ValueError("topic 不能为空。")

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO update_logs (
                    topic,
                    source,
                    query_used,
                    fetched_count,
                    inserted_count,
                    skipped_count
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    topic,
                    source,
                    query_used,
                    int(fetched_count),
                    int(inserted_count),
                    int(skipped_count),
                )
            )
            conn.commit()
            return cursor.lastrowid

    def get_latest_log_by_topic(self, topic: str) -> Optional[Dict[str, Any]]:
        if not topic:
            return None

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT *
                FROM update_logs
                WHERE topic = ?
                ORDER BY update_time DESC, id DESC
                LIMIT 1
                """,
                (topic,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def list_logs_by_topic(
        self,
        topic: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        if not topic:
            return []

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT *
                FROM update_logs
                WHERE topic = ?
                ORDER BY update_time DESC, id DESC
                LIMIT ?
                """,
                (topic, limit)
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def list_recent_logs(self, limit: int = 20) -> List[Dict[str, Any]]:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT *
                FROM update_logs
                ORDER BY update_time DESC, id DESC
                LIMIT ?
                """,
                (limit,)
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_latest_update_time_by_topic(self, topic: str) -> Optional[str]:
        latest = self.get_latest_log_by_topic(topic)
        if not latest:
            return None
        return latest.get("update_time")