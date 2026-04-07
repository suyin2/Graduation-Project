from typing import Any, Dict, List, Tuple
from datetime import datetime

from data_resourses.get_data_arxiv import ArxivFetcher


class PaperFetchService:
    """
    论文抓取服务：
    - 根据 query_info 调用 arXiv 抓取器
    - 尽量兼容旧版 get_data_arxiv.py 的字段
    - 不负责清洗、不负责入库
    - 目标：尽量不向上抛网络异常，保证部分成功结果可以继续返回
    """

    def __init__(
        self,
        max_results: int = 10,
        sort_mode: str = "最新"
    ):
        self.max_results = max_results
        self.sort_mode = sort_mode
        self.fetcher = ArxivFetcher(
            max_results=max_results,
            sort_mode=sort_mode
        )

    def fetch_papers(
        self,
        query_info: Dict[str, Any],
        use_default_5y: bool = False
    ) -> Tuple[List[Dict[str, Any]], str]:
        if not isinstance(query_info, dict):
            raise ValueError("query_info 必须是字典。")

        try:
            papers, used_query = self.fetcher.fetch_with_fallback(
                query_info,
                use_default_5y=use_default_5y
            )
            return self._normalize_papers(papers), used_query or ""
        except Exception as e:
            print(f"[WARN] fetch_papers 抓取失败：{e}")
            return [], ""

    def fetch_papers_by_date_range(
        self,
        query_info: Dict[str, Any],
        start_date: datetime,
        end_date: datetime
    ) -> Tuple[List[Dict[str, Any]], str]:
        if not isinstance(query_info, dict):
            raise ValueError("query_info 必须是字典。")

        try:
            papers, used_query = self.fetcher.fetch_with_fallback(
                query_info,
                start_date=start_date,
                end_date=end_date
            )
            return self._normalize_papers(papers), used_query or ""
        except Exception as e:
            print(
                f"[WARN] fetch_papers_by_date_range 抓取失败："
                f"{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}，{e}"
            )
            return [], ""

    def fetch_papers_by_year(
        self,
        query_info: Dict[str, Any],
        year: int
    ) -> Tuple[List[Dict[str, Any]], str]:
        """
        单年抓取：
        - 单年失败只返回空，不向上抛异常
        - 这样 AnalysisService 在按年份循环时，前面成功年份的结果仍可保留
        """
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)

        try:
            return self.fetch_papers_by_date_range(
                query_info=query_info,
                start_date=start_date,
                end_date=end_date
            )
        except Exception as e:
            print(f"[WARN] fetch_papers_by_year 失败，year={year}：{e}")
            return [], ""

    def _normalize_papers(self, papers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not papers:
            return []

        normalized_papers = [self._normalize_raw_paper(item) for item in papers]
        normalized_papers = [item for item in normalized_papers if item]

        deduped: List[Dict[str, Any]] = []
        seen_keys = set()

        for paper in normalized_papers:
            dedup_key = (
                paper.get("paper_id")
                or paper.get("link")
                or paper.get("title", "").strip().lower()
            )
            if not dedup_key or dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)
            deduped.append(paper)

        return deduped

    def _normalize_raw_paper(self, paper: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(paper, dict):
            return {}

        title = (paper.get("title", "") or "").strip()
        link = (paper.get("link", "") or "").strip()
        paper_id = (paper.get("paper_id", "") or "").strip()

        if not title and not link and not paper_id:
            return {}

        return {
            "paper_id": paper_id,
            "source": paper.get("source", "arxiv"),
            "title": title,
            "authors": paper.get("authors", ""),
            "summary": paper.get("summary", paper.get("abstract", "")),
            "published": paper.get("published", paper.get("published_date", "")),
            "updated": paper.get("updated", paper.get("updated_date", "")),
            "link": link,
            "pdf_url": paper.get("pdf_url", ""),
            "pdf_local_path": paper.get("pdf_local_path", ""),
        }