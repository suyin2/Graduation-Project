from typing import Any, Dict, List, Optional

from data_resourses.get_data_openalex import OpenAlexFetcher
from services.paper_ingest_service import PaperIngestService


class OpenAlexImportService:
    """
    OpenAlex -> 元数据标准化 -> 复用现有 PaperIngestService 入库
    """

    def __init__(
        self,
        per_page: int = 25,
        api_key: str = "",
        mailto: str = ""
    ):
        self.fetcher = OpenAlexFetcher(
            per_page=per_page,
            api_key=api_key,
            mailto=mailto
        )
        self.ingest_service = PaperIngestService()

    def import_by_keyword(
        self,
        keyword: str,
        topic: str = "",
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        max_results: int = 100,
        is_oa_only: bool = False
    ) -> Dict[str, Any]:
        papers, fetch_meta = self.fetcher.search_works(
            keyword=keyword,
            start_year=start_year,
            end_year=end_year,
            max_results=max_results,
            is_oa_only=is_oa_only
        )

        ingest_topic = (topic or keyword or "").strip()

        ingest_stats = self.ingest_service.ingest_papers(
            raw_papers=papers,
            topic=ingest_topic,
            source="openalex",
            query_used=self._build_query_desc(
                keyword=keyword,
                start_year=start_year,
                end_year=end_year,
                max_results=max_results,
                is_oa_only=is_oa_only
            )
        )

        return {
            "keyword": keyword,
            "topic": ingest_topic,
            "fetch_meta": fetch_meta,
            "fetched_papers_count": len(papers),
            "fetched_papers_preview": papers[:3],
            "ingest_stats": ingest_stats,
        }

    def _build_query_desc(
        self,
        keyword: str,
        start_year: Optional[int],
        end_year: Optional[int],
        max_results: int,
        is_oa_only: bool
    ) -> str:
        parts = [f"search={keyword}", f"max_results={max_results}"]
        if start_year is not None:
            parts.append(f"start_year={start_year}")
        if end_year is not None:
            parts.append(f"end_year={end_year}")
        if is_oa_only:
            parts.append("is_oa_only=true")
        return " | ".join(parts)