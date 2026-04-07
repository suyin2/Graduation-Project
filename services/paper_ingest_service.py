from typing import Any, Dict, List

from database.paper_repository import PaperRepository
from database.category_repository import CategoryRepository
from database.tag_repository import TagRepository
from database.update_log_repository import UpdateLogRepository
from services.paper_clean_service import PaperCleanService
from services.paper_classify_service import PaperClassifyService


class PaperIngestService:
    """
    论文入库编排服务：
    - 清洗抓取结果
    - 分类与标签生成
    - 去重判断
    - 写入 papers / categories / tags
    - 写入 update_logs

    兼容旧版 arXiv 返回字段：
    - summary
    - published
    """

    def __init__(self):
        self.paper_repo = PaperRepository()
        self.category_repo = CategoryRepository()
        self.tag_repo = TagRepository()
        self.update_log_repo = UpdateLogRepository()
        self.clean_service = PaperCleanService()
        self.classify_service = PaperClassifyService()

    def ingest_papers(
        self,
        raw_papers: List[Dict[str, Any]],
        topic: str,
        source: str = "arxiv",
        query_used: str = ""
    ) -> Dict[str, Any]:
        cleaned_papers = self.clean_service.clean_papers(raw_papers)

        stats = {
            "topic": topic,
            "source": source,
            "query_used": query_used,
            "fetched_count": len(cleaned_papers),
            "inserted_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
            "inserted_paper_ids": [],
        }

        for paper in cleaned_papers:
            try:
                paper_id = paper.get("paper_id", "")
                link = paper.get("link", "")

                exists = False
                if paper_id and self.paper_repo.exists_by_paper_id(paper_id):
                    exists = True
                elif link and self.paper_repo.exists_by_link(link):
                    exists = True

                if exists:
                    stats["skipped_count"] += 1
                    continue

                classify_result = self.classify_service.classify_paper(paper)

                main_topic = classify_result.get("main_topic", "")
                sub_topic = classify_result.get("sub_topic", "")
                contribution_summary = classify_result.get("contribution_summary", "")
                method_summary = classify_result.get("method_summary", "")
                application_scenario = classify_result.get("application_scenario", "")
                timeline_stage = classify_result.get("timeline_stage", "")

                # 这里传入的 paper 已经是 clean_service 处理后的统一格式
                # 同时保留了旧字段和规范字段
                self.paper_repo.insert_paper(
                    paper=paper,
                    main_topic=main_topic,
                    sub_topic=sub_topic,
                    contribution_summary=contribution_summary,
                    method_summary=method_summary,
                    application_scenario=application_scenario,
                    timeline_stage=timeline_stage,
                    importance_score=0,
                    embedding_status=0,
                    full_text_status=0,
                )

                categories = classify_result.get("categories", [])
                tags = classify_result.get("tags", [])

                if paper_id and categories:
                    self.category_repo.bulk_insert_categories(paper_id, categories)

                if paper_id and tags:
                    self.tag_repo.bulk_insert_tags(paper_id, tags)

                stats["inserted_count"] += 1
                if paper_id:
                    stats["inserted_paper_ids"].append(paper_id)

            except Exception:
                stats["failed_count"] += 1

        self.update_log_repo.insert_log(
            topic=topic,
            source=source,
            query_used=query_used,
            fetched_count=stats["fetched_count"],
            inserted_count=stats["inserted_count"],
            skipped_count=stats["skipped_count"],
        )

        return stats