from typing import Any, Dict, List

from database.db import get_connection


class TagRepository:
    """
    论文标签仓库层：
    - 插入标签
    - 批量插入标签
    - 删除某论文已有标签
    - 查询标签
    - 统计高频标签
    """

    def insert_tag(
        self,
        paper_id: str,
        tag: str,
        tag_type: str = "keyword"
    ) -> int:
        if not paper_id or not tag:
            raise ValueError("paper_id 和 tag 不能为空。")

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO paper_tags (
                    paper_id,
                    tag,
                    tag_type
                )
                VALUES (?, ?, ?)
                """,
                (
                    paper_id,
                    tag.strip(),
                    tag_type.strip() if tag_type else "keyword",
                )
            )
            conn.commit()
            return cursor.lastrowid

    def bulk_insert_tags(
        self,
        paper_id: str,
        tags: List[Dict[str, Any]]
    ) -> None:
        """
        tags 示例：
        [
            {"tag": "RAG", "tag_type": "keyword"},
            {"tag": "query rewriting", "tag_type": "method"}
        ]
        """
        if not paper_id or not tags:
            return

        with get_connection() as conn:
            cursor = conn.cursor()
            for item in tags:
                tag = item.get("tag", "").strip()
                tag_type = item.get("tag_type", "keyword").strip()

                if not tag:
                    continue

                cursor.execute(
                    """
                    INSERT INTO paper_tags (
                        paper_id,
                        tag,
                        tag_type
                    )
                    VALUES (?, ?, ?)
                    """,
                    (
                        paper_id,
                        tag,
                        tag_type,
                    )
                )
            conn.commit()

    def delete_by_paper_id(self, paper_id: str) -> None:
        if not paper_id:
            return

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM paper_tags WHERE paper_id = ?",
                (paper_id,)
            )
            conn.commit()

    def replace_tags(
        self,
        paper_id: str,
        tags: List[Dict[str, Any]]
    ) -> None:
        """
        先删后插，适合重新打标签。
        """
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM paper_tags WHERE paper_id = ?",
                (paper_id,)
            )

            for item in tags:
                tag = item.get("tag", "").strip()
                tag_type = item.get("tag_type", "keyword").strip()

                if not tag:
                    continue

                cursor.execute(
                    """
                    INSERT INTO paper_tags (
                        paper_id,
                        tag,
                        tag_type
                    )
                    VALUES (?, ?, ?)
                    """,
                    (
                        paper_id,
                        tag,
                        tag_type,
                    )
                )
            conn.commit()

    def get_tags_by_paper_id(self, paper_id: str) -> List[Dict[str, Any]]:
        if not paper_id:
            return []

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM paper_tags
                WHERE paper_id = ?
                ORDER BY id ASC
                """,
                (paper_id,)
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_papers_by_tag(
        self,
        tag: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        if not tag:
            return []

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT p.*
                FROM papers p
                INNER JOIN paper_tags t
                    ON p.paper_id = t.paper_id
                WHERE t.tag = ?
                ORDER BY p.published_date DESC
                LIMIT ?
                """,
                (tag, limit)
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def count_top_tags(
        self,
        limit: int = 20,
        tag_type: str = ""
    ) -> List[Dict[str, Any]]:
        with get_connection() as conn:
            cursor = conn.cursor()

            if tag_type:
                cursor.execute(
                    """
                    SELECT tag, tag_type, COUNT(*) AS freq
                    FROM paper_tags
                    WHERE tag_type = ?
                    GROUP BY tag, tag_type
                    ORDER BY freq DESC, tag ASC
                    LIMIT ?
                    """,
                    (tag_type, limit)
                )
            else:
                cursor.execute(
                    """
                    SELECT tag, tag_type, COUNT(*) AS freq
                    FROM paper_tags
                    GROUP BY tag, tag_type
                    ORDER BY freq DESC, tag ASC
                    LIMIT ?
                    """,
                    (limit,)
                )

            rows = cursor.fetchall()
            return [dict(row) for row in rows]