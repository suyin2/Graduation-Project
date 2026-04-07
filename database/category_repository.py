from typing import Any, Dict, List

from database.db import get_connection


class CategoryRepository:
    """
    论文分类仓库层：
    - 插入分类
    - 删除某论文已有分类
    - 查询分类
    - 按分类取论文
    """

    def insert_category(
        self,
        paper_id: str,
        category_type: str,
        category_value: str,
        confidence: float = 0,
        source_method: str = "rule"
    ) -> int:
        if not paper_id or not category_type or not category_value:
            raise ValueError("paper_id、category_type、category_value 不能为空。")

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO paper_categories (
                    paper_id,
                    category_type,
                    category_value,
                    confidence,
                    source_method
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    paper_id,
                    category_type,
                    category_value,
                    confidence,
                    source_method,
                )
            )
            conn.commit()
            return cursor.lastrowid

    def bulk_insert_categories(
        self,
        paper_id: str,
        categories: List[Dict[str, Any]]
    ) -> None:
        """
        categories 示例：
        [
            {"category_type": "main_topic", "category_value": "RAG", "confidence": 0.9, "source_method": "rule"},
            {"category_type": "sub_topic", "category_value": "query rewriting", "confidence": 0.8, "source_method": "llm"}
        ]
        """
        if not paper_id or not categories:
            return

        with get_connection() as conn:
            cursor = conn.cursor()
            for item in categories:
                category_type = item.get("category_type", "").strip()
                category_value = item.get("category_value", "").strip()
                confidence = float(item.get("confidence", 0) or 0)
                source_method = item.get("source_method", "rule")

                if not category_type or not category_value:
                    continue

                cursor.execute(
                    """
                    INSERT INTO paper_categories (
                        paper_id,
                        category_type,
                        category_value,
                        confidence,
                        source_method
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        paper_id,
                        category_type,
                        category_value,
                        confidence,
                        source_method,
                    )
                )
            conn.commit()

    def delete_by_paper_id(self, paper_id: str) -> None:
        if not paper_id:
            return

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM paper_categories WHERE paper_id = ?",
                (paper_id,)
            )
            conn.commit()

    def replace_categories(
        self,
        paper_id: str,
        categories: List[Dict[str, Any]]
    ) -> None:
        """
        先删后插，适合重新分类。
        """
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM paper_categories WHERE paper_id = ?",
                (paper_id,)
            )

            for item in categories:
                category_type = item.get("category_type", "").strip()
                category_value = item.get("category_value", "").strip()
                confidence = float(item.get("confidence", 0) or 0)
                source_method = item.get("source_method", "rule")

                if not category_type or not category_value:
                    continue

                cursor.execute(
                    """
                    INSERT INTO paper_categories (
                        paper_id,
                        category_type,
                        category_value,
                        confidence,
                        source_method
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        paper_id,
                        category_type,
                        category_value,
                        confidence,
                        source_method,
                    )
                )

            conn.commit()

    def get_categories_by_paper_id(self, paper_id: str) -> List[Dict[str, Any]]:
        if not paper_id:
            return []

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM paper_categories
                WHERE paper_id = ?
                ORDER BY id ASC
                """,
                (paper_id,)
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_papers_by_category(
        self,
        category_type: str,
        category_value: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        按分类取论文主表信息。
        """
        if not category_type or not category_value:
            return []

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT p.*
                FROM papers p
                INNER JOIN paper_categories c
                    ON p.paper_id = c.paper_id
                WHERE c.category_type = ?
                  AND c.category_value = ?
                ORDER BY p.published_date DESC
                LIMIT ?
                """,
                (category_type, category_value, limit)
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]