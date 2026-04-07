from typing import Any, Dict, List, Optional

from database.db import get_connection


class PaperRepository:
    """
    论文主表仓库层：
    - 检查是否存在
    - 插入论文
    - 查询论文
    - 按主题/时间获取论文
    """

    def exists_by_paper_id(self, paper_id: str) -> bool:
        if not paper_id:
            return False

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM papers WHERE paper_id = ? LIMIT 1",
                (paper_id,)
            )
            return cursor.fetchone() is not None

    def exists_by_link(self, link: str) -> bool:
        if not link:
            return False

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM papers WHERE link = ? LIMIT 1",
                (link,)
            )
            return cursor.fetchone() is not None

    def get_by_paper_id(self, paper_id: str) -> Optional[Dict[str, Any]]:
        if not paper_id:
            return None

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM papers WHERE paper_id = ? LIMIT 1",
                (paper_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_by_link(self, link: str) -> Optional[Dict[str, Any]]:
        if not link:
            return None

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM papers WHERE link = ? LIMIT 1",
                (link,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def insert_paper(
        self,
        paper: Dict[str, Any],
        main_topic: str = "",
        sub_topic: str = "",
        contribution_summary: str = "",
        method_summary: str = "",
        application_scenario: str = "",
        timeline_stage: str = "",
        importance_score: float = 0,
        embedding_status: int = 0,
        full_text_status: int = 0,
    ) -> int:
        """
        插入论文主表。
        若 paper_id 或 link 已存在，建议外部先判断，避免重复插入。
        返回插入后的自增 id。

        兼容两套字段：
        - 旧字段：summary / published / updated
        - 新字段：abstract / published_date / updated_date
        """
        abstract = paper.get("abstract", paper.get("summary", ""))
        published_date = paper.get("published_date", paper.get("published", ""))
        updated_date = paper.get("updated_date", paper.get("updated", ""))

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO papers (
                    paper_id,
                    source,
                    title,
                    authors,
                    abstract,
                    published_date,
                    updated_date,
                    link,
                    pdf_url,
                    pdf_local_path,
                    main_topic,
                    sub_topic,
                    contribution_summary,
                    method_summary,
                    application_scenario,
                    timeline_stage,
                    importance_score,
                    embedding_status,
                    full_text_status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    paper.get("paper_id", ""),
                    paper.get("source", "arxiv"),
                    paper.get("title", ""),
                    paper.get("authors", ""),
                    abstract,
                    published_date,
                    updated_date,
                    paper.get("link", ""),
                    paper.get("pdf_url", ""),
                    paper.get("pdf_local_path", ""),
                    main_topic,
                    sub_topic,
                    contribution_summary,
                    method_summary,
                    application_scenario,
                    timeline_stage,
                    importance_score,
                    embedding_status,
                    full_text_status,
                )
            )
            conn.commit()
            return cursor.lastrowid

    def update_main_fields(
        self,
        paper_id: str,
        main_topic: str = "",
        sub_topic: str = "",
        contribution_summary: str = "",
        method_summary: str = "",
        application_scenario: str = "",
        timeline_stage: str = "",
        importance_score: float = 0,
    ) -> None:
        """
        更新主表中的结构化分析字段。
        """
        if not paper_id:
            return

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE papers
                SET
                    main_topic = ?,
                    sub_topic = ?,
                    contribution_summary = ?,
                    method_summary = ?,
                    application_scenario = ?,
                    timeline_stage = ?,
                    importance_score = ?,
                    updated_at_local = CURRENT_TIMESTAMP
                WHERE paper_id = ?
                """,
                (
                    main_topic,
                    sub_topic,
                    contribution_summary,
                    method_summary,
                    application_scenario,
                    timeline_stage,
                    importance_score,
                    paper_id,
                )
            )
            conn.commit()

    def get_papers_by_main_topic(
        self,
        main_topic: str,
        limit: int = 20,
        order_desc: bool = True
    ) -> List[Dict[str, Any]]:
        order = "DESC" if order_desc else "ASC"

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT * FROM papers
                WHERE main_topic = ?
                ORDER BY published_date {order}
                LIMIT ?
                """,
                (main_topic, limit)
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_papers_by_sub_topic(
        self,
        sub_topic: str,
        limit: int = 20,
        order_desc: bool = True
    ) -> List[Dict[str, Any]]:
        order = "DESC" if order_desc else "ASC"

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT * FROM papers
                WHERE sub_topic = ?
                ORDER BY published_date {order}
                LIMIT ?
                """,
                (sub_topic, limit)
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def search_papers_by_keyword(
        self,
        keyword: str,
        limit: int = 20,
        order_desc: bool = True
    ) -> List[Dict[str, Any]]:
        """
        在 title / abstract / main_topic / sub_topic 中做简单模糊匹配。
        第一版够用了。
        """
        if not keyword:
            return []

        order = "DESC" if order_desc else "ASC"
        like_keyword = f"%{keyword}%"

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT * FROM papers
                WHERE
                    title LIKE ?
                    OR abstract LIKE ?
                    OR main_topic LIKE ?
                    OR sub_topic LIKE ?
                ORDER BY published_date {order}
                LIMIT ?
                """,
                (
                    like_keyword,
                    like_keyword,
                    like_keyword,
                    like_keyword,
                    limit,
                )
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_latest_published_date_by_topic(self, main_topic: str) -> Optional[str]:
        if not main_topic:
            return None

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT MAX(published_date) AS latest_date
                FROM papers
                WHERE main_topic = ?
                """,
                (main_topic,)
            )
            row = cursor.fetchone()
            return row["latest_date"] if row and row["latest_date"] else None

    def get_paper_count_by_topic(self, main_topic: str) -> int:
        if not main_topic:
            return 0

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM papers
                WHERE main_topic = ?
                """,
                (main_topic,)
            )
            row = cursor.fetchone()
            return int(row["cnt"]) if row else 0

    def get_papers_for_timeline(
        self,
        main_topic: str,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        用于时间线分析：按发布时间升序获取。
        """
        if not main_topic:
            return []

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM papers
                WHERE main_topic = ?
                ORDER BY published_date ASC
                LIMIT ?
                """,
                (main_topic, limit)
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def list_recent_papers(self, limit: int = 20) -> List[Dict[str, Any]]:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM papers
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,)
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]