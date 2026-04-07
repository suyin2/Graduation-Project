import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests


class OpenAlexFetcher:
    """
    OpenAlex 元数据获取器
    - 使用 /works 接口
    - 支持 search + publication_year 过滤
    - 支持基础分页
    - 返回统一后的论文元数据结构，便于复用现有 PaperIngestService
    """

    BASE_URL = "https://api.openalex.org"
    WORKS_ENDPOINT = "/works"

    REQUEST_TIMEOUT = 30
    MIN_REQUEST_INTERVAL_SECONDS = 1.2
    MAX_RETRIES = 3
    RETRY_SLEEP_SECONDS = 3

    def __init__(
        self,
        per_page: int = 25,
        api_key: str = "",
        mailto: str = ""
    ):
        # OpenAlex per_page 最大 100
        self.per_page = max(1, min(100, int(per_page)))
        self.api_key = (api_key or "").strip()
        self.mailto = (mailto or "").strip()

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; OpenAlexImporter/1.0)"
        })

        self._last_request_ts: Optional[float] = None

    def _wait_rate_limit(self):
        if self._last_request_ts is None:
            return

        elapsed = time.time() - self._last_request_ts
        if elapsed < self.MIN_REQUEST_INTERVAL_SECONDS:
            time.sleep(self.MIN_REQUEST_INTERVAL_SECONDS - elapsed)

    def _request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        last_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                self._wait_rate_limit()
                self._last_request_ts = time.time()

                url = f"{self.BASE_URL}{self.WORKS_ENDPOINT}"
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.REQUEST_TIMEOUT
                )
                response.raise_for_status()
                return response.json()

            except requests.RequestException as e:
                last_error = e
                print(f"[OpenAlex] 请求失败（第 {attempt}/{self.MAX_RETRIES} 次）：{e}")

                if attempt < self.MAX_RETRIES:
                    time.sleep(self.RETRY_SLEEP_SECONDS * attempt)

        raise RuntimeError(f"OpenAlex 请求最终失败：{last_error}")

    def _build_filter(
        self,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        is_oa_only: bool = False,
        has_abstract: bool = True
    ) -> str:
        filters = []

        if start_year is not None:
            filters.append(f"publication_year:>{start_year - 1}")
        if end_year is not None:
            filters.append(f"publication_year:<{end_year + 1}")
        if is_oa_only:
            filters.append("is_oa:true")
        if has_abstract:
            filters.append("has_abstract:true")

        return ",".join(filters)

    def search_works(
        self,
        keyword: str,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        max_results: int = 100,
        is_oa_only: bool = False
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        按关键词搜索 works，并自动分页直到达到 max_results 或结果耗尽。
        """
        keyword = (keyword or "").strip()
        if not keyword:
            raise ValueError("keyword 不能为空")

        target_count = max(1, int(max_results))
        all_results: List[Dict[str, Any]] = []

        page = 1
        meta_info: Dict[str, Any] = {
            "keyword": keyword,
            "start_year": start_year,
            "end_year": end_year,
            "requested_max_results": target_count,
            "pages_fetched": 0,
        }

        while len(all_results) < target_count:
            params: Dict[str, Any] = {
                "search": keyword,
                "per_page": min(self.per_page, target_count - len(all_results)),
                "page": page,
                "sort": "publication_date:desc",
            }

            filter_value = self._build_filter(
                start_year=start_year,
                end_year=end_year,
                is_oa_only=is_oa_only,
                has_abstract=True
            )
            if filter_value:
                params["filter"] = filter_value

            if self.api_key:
                params["api_key"] = self.api_key
            if self.mailto:
                params["mailto"] = self.mailto

            data = self._request(params)
            page_results = data.get("results", []) or []

            meta_info["pages_fetched"] += 1

            if not page_results:
                break

            all_results.extend(page_results)

            if len(page_results) < params["per_page"]:
                break

            page += 1

        normalized = [self._normalize_work(item) for item in all_results]
        normalized = [item for item in normalized if item]

        return normalized[:target_count], meta_info

    def _normalize_work(self, work: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(work, dict):
            return {}

        openalex_id = str(work.get("id", "") or "").strip()
        ids_obj = work.get("ids", {}) or {}
        doi = str(ids_obj.get("doi", "") or "").strip()

        title = str(work.get("display_name", "") or "").strip()
        publication_date = str(work.get("publication_date", "") or "").strip()

        updated_date = str(work.get("updated_date", "") or "").strip()

        abstract = self._extract_abstract(work)
        authors = self._extract_authors(work)
        link = self._extract_best_link(work)
        pdf_url = self._extract_pdf_url(work)

        if not openalex_id and not doi and not title:
            return {}

        paper_id = doi or openalex_id or title.lower()

        primary_topic = self._extract_primary_topic(work)
        main_topic = primary_topic.get("field", "")
        sub_topic = primary_topic.get("topic", "")

        return {
            "paper_id": paper_id,
            "source": "openalex",
            "title": title,
            "authors": authors,
            "abstract": abstract,
            "summary": abstract,  # 兼容旧链路
            "published_date": publication_date,
            "published": publication_date,  # 兼容旧链路
            "updated_date": updated_date,
            "updated": updated_date,  # 兼容旧链路
            "link": link,
            "pdf_url": pdf_url,
            "pdf_local_path": "",
            "main_topic": main_topic,
            "sub_topic": sub_topic,
            "openalex_id": openalex_id,
            "doi": doi,
        }

    def _extract_authors(self, work: Dict[str, Any]) -> str:
        authorships = work.get("authorships", []) or []
        names = []

        for item in authorships:
            author = item.get("author", {}) or {}
            name = str(author.get("display_name", "") or "").strip()
            if name:
                names.append(name)

        return ", ".join(names)

    def _extract_abstract(self, work: Dict[str, Any]) -> str:
        """
        OpenAlex 常见摘要形式是 abstract_inverted_index，需要还原。
        """
        inverted = work.get("abstract_inverted_index", None)
        if not inverted or not isinstance(inverted, dict):
            return ""

        positions = []
        for word, pos_list in inverted.items():
            if not isinstance(pos_list, list):
                continue
            for pos in pos_list:
                if isinstance(pos, int):
                    positions.append((pos, word))

        if not positions:
            return ""

        positions.sort(key=lambda x: x[0])
        return " ".join(word for _, word in positions)

    def _extract_best_link(self, work: Dict[str, Any]) -> str:
        doi = str((work.get("ids", {}) or {}).get("doi", "") or "").strip()
        if doi:
            return doi

        primary_location = work.get("primary_location", {}) or {}
        landing_page_url = str(primary_location.get("landing_page_url", "") or "").strip()
        if landing_page_url:
            return landing_page_url

        openalex_id = str(work.get("id", "") or "").strip()
        return openalex_id

    def _extract_pdf_url(self, work: Dict[str, Any]) -> str:
        primary_location = work.get("primary_location", {}) or {}
        pdf_url = str(primary_location.get("pdf_url", "") or "").strip()
        if pdf_url:
            return pdf_url

        best_oa = work.get("best_oa_location", {}) or {}
        pdf_url = str(best_oa.get("pdf_url", "") or "").strip()
        if pdf_url:
            return pdf_url

        return ""

    def _extract_primary_topic(self, work: Dict[str, Any]) -> Dict[str, str]:
        primary_topic = work.get("primary_topic", {}) or {}
        if not isinstance(primary_topic, dict):
            return {"field": "", "topic": ""}

        field_obj = primary_topic.get("field", {}) or {}
        field_name = str(field_obj.get("display_name", "") or "").strip()
        topic_name = str(primary_topic.get("display_name", "") or "").strip()

        return {
            "field": field_name,
            "topic": topic_name,
        }