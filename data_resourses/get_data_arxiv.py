import random
import time
from datetime import datetime
from urllib.parse import quote

import feedparser
import requests


class ArxivFetcher:
    """
    arXiv 数据获取类
    支持：
    1. 根据关键词获取论文信息
    2. 根据排序模式控制 arXiv 返回顺序
    3. 根据时间范围筛选论文
    4. 严格全局限速：任意两次 HTTP 请求之间至少间隔 3 秒
    5. 对 429 / timeout 做更保守的重试与退避
    6. 单个候选词失败时继续 fallback，尽量返回部分成功结果
    """

    BASE_URL = "https://export.arxiv.org/api/query"

    SORT_MODE_MAPPING = {
        "相关性": ("relevance", "descending"),
        "最新": ("submittedDate", "descending"),
        "最早": ("submittedDate", "ascending"),
    }

    # 官方要求：不要快于每 3 秒 1 次请求
    MIN_REQUEST_INTERVAL_SECONDS = 3.2

    # 更保守一点，减少 timeout 误伤
    REQUEST_TIMEOUT = 30

    # 单次请求最大重试次数
    MAX_RETRIES = 3

    # 普通失败基础退避
    RETRY_BASE_SLEEP_SECONDS = 6

    # 429 退避更重
    RETRY_429_BASE_SLEEP_SECONDS = 15

    def __init__(self, max_results=5, sort_mode="最新"):
        self.max_results = max_results
        self.sort_mode = sort_mode if sort_mode in self.SORT_MODE_MAPPING else "最新"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; PaperFetcher/1.0; +arxiv)"
        })

        # 记录最近一次实际发出 HTTP 请求的时间
        self._last_request_ts = None

    def _get_sort_params(self):
        return self.SORT_MODE_MAPPING.get(self.sort_mode, ("submittedDate", "descending"))

    def _format_arxiv_date(self, dt: datetime, end_of_day: bool = False) -> str:
        if end_of_day:
            return dt.strftime("%Y%m%d2359")
        return dt.strftime("%Y%m%d0000")

    def _get_default_5y_range(self):
        now = datetime.now()
        try:
            start_dt = now.replace(year=now.year - 5)
        except ValueError:
            start_dt = now.replace(year=now.year - 5, day=28)
        return start_dt, now

    def _normalize_keyword(self, keyword: str) -> str:
        text = (keyword or "").strip()
        if not text:
            return ""

        if text.startswith("(") and text.endswith(")"):
            return text

        return f"all:({text})"

    def _build_search_query(self, keyword, start_date=None, end_date=None, use_default_5y=False):
        query = self._normalize_keyword(keyword)

        if use_default_5y and (start_date is None and end_date is None):
            start_date, end_date = self._get_default_5y_range()

        if start_date and end_date:
            start_str = self._format_arxiv_date(start_date, end_of_day=False)
            end_str = self._format_arxiv_date(end_date, end_of_day=True)
            query += f" AND submittedDate:[{start_str} TO {end_str}]"

        return query

    def _build_url(self, keyword, start_date=None, end_date=None, use_default_5y=False):
        search_query = self._build_search_query(
            keyword=keyword,
            start_date=start_date,
            end_date=end_date,
            use_default_5y=use_default_5y
        )

        encoded_query = quote(search_query, safe="():[]\"")
        sort_by, sort_order = self._get_sort_params()

        url = (
            f"{self.BASE_URL}"
            f"?search_query={encoded_query}"
            f"&start=0"
            f"&max_results={self.max_results}"
            f"&sortBy={sort_by}"
            f"&sortOrder={sort_order}"
        )
        return url

    def _wait_for_global_rate_limit(self):
        """
        严格保证任意两次真实 HTTP 请求之间至少间隔 MIN_REQUEST_INTERVAL_SECONDS。
        """
        now = time.time()

        if self._last_request_ts is None:
            return

        elapsed = now - self._last_request_ts
        if elapsed < self.MIN_REQUEST_INTERVAL_SECONDS:
            need_sleep = self.MIN_REQUEST_INTERVAL_SECONDS - elapsed
            print(f"全局限速生效，等待 {need_sleep:.1f} 秒后再请求……")
            time.sleep(need_sleep)

    def _calc_retry_sleep(self, attempt: int, is_429: bool = False) -> float:
        """
        指数退避 + 抖动
        """
        base = self.RETRY_429_BASE_SLEEP_SECONDS if is_429 else self.RETRY_BASE_SLEEP_SECONDS
        sleep_seconds = base * (2 ** (attempt - 1))
        jitter = random.uniform(0.5, 1.5)
        return sleep_seconds + jitter

    def _request_with_retry(self, url: str) -> str:
        last_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                # 每次真正发请求前，统一做全局限速
                self._wait_for_global_rate_limit()
                self._last_request_ts = time.time()

                response = self.session.get(url, timeout=self.REQUEST_TIMEOUT)
                status_code = response.status_code

                if status_code == 429:
                    raise requests.HTTPError("429 Too Many Requests", response=response)

                response.raise_for_status()
                return response.text

            except requests.RequestException as e:
                last_error = e

                response = getattr(e, "response", None)
                status_code = getattr(response, "status_code", None)
                is_429 = status_code == 429 or "429" in str(e)

                print(f"请求失败（第 {attempt}/{self.MAX_RETRIES} 次）：{e}")

                if attempt < self.MAX_RETRIES:
                    sleep_seconds = self._calc_retry_sleep(attempt, is_429=is_429)
                    print(f"等待 {sleep_seconds:.1f} 秒后重试……")
                    time.sleep(sleep_seconds)

        print("请求最终失败：", last_error)
        return ""

    def fetch(self, keyword, start_date=None, end_date=None, use_default_5y=False):
        keyword = (keyword or "").strip()
        if not keyword:
            return []

        url = self._build_url(
            keyword=keyword,
            start_date=start_date,
            end_date=end_date,
            use_default_5y=use_default_5y
        )

        response_text = self._request_with_retry(url)
        if not response_text:
            return []

        try:
            feed = feedparser.parse(response_text)
        except Exception as e:
            print(f"[WARN] feed 解析失败：{e}")
            return []

        papers = []

        for entry in getattr(feed, "entries", []):
            try:
                paper = {
                    "title": getattr(entry, "title", "").replace("\n", " ").strip(),
                    "authors": ", ".join(
                        author.name for author in getattr(entry, "authors", [])
                        if getattr(author, "name", "")
                    ),
                    "summary": getattr(entry, "summary", "").replace("\n", " ").strip(),
                    "published": getattr(entry, "published", ""),
                    "updated": getattr(entry, "updated", getattr(entry, "published", "")),
                    "link": getattr(entry, "link", ""),
                    "paper_id": getattr(entry, "id", ""),
                    "source": "arxiv",
                    "pdf_url": self._extract_pdf_url(entry),
                }

                if paper["title"] or paper["link"] or paper["paper_id"]:
                    papers.append(paper)
            except Exception as e:
                print(f"[WARN] 单篇论文解析失败，已跳过：{e}")
                continue

        return papers

    def _extract_pdf_url(self, entry):
        links = getattr(entry, "links", [])
        for link in links:
            href = getattr(link, "href", "")
            link_type = getattr(link, "type", "")
            title = getattr(link, "title", "")

            if "pdf" in href.lower() or link_type == "application/pdf" or title == "pdf":
                return href

        return ""

    def _normalize_candidate_for_dedup(self, text: str) -> str:
        return " ".join((text or "").strip().lower().split())

    def _build_fallback_candidates(self, query_info: dict):
        candidates = [
            str(query_info.get("paper_query", "") or "").strip(),
            str(query_info.get("topic_en", "") or "").strip(),
            str(query_info.get("topic_zh", "") or "").strip()
        ]

        deduped = []
        seen = set()

        for q in candidates:
            norm_q = self._normalize_candidate_for_dedup(q)
            if not norm_q or norm_q in seen:
                continue
            seen.add(norm_q)
            deduped.append(q)

        return deduped

    def fetch_with_fallback(self, query_info: dict, start_date=None, end_date=None, use_default_5y=False):
        """
        优先级：
        1. paper_query
        2. topic_en
        3. topic_zh

        目标：
        - 单个候选失败不抛异常
        - 有一个候选成功拿到结果就返回
        - 都失败则返回空
        """
        candidates = self._build_fallback_candidates(query_info)

        for q in candidates:
            try:
                papers = self.fetch(
                    q,
                    start_date=start_date,
                    end_date=end_date,
                    use_default_5y=use_default_5y
                )
                if papers:
                    return papers, q
            except Exception as e:
                print(f"[WARN] fallback 候选抓取失败，query={q}：{e}")
                continue

        return [], ""