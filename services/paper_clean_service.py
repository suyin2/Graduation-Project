import re
from typing import Any, Dict, List
from urllib.parse import urlparse


class PaperCleanService:
    """
    论文清洗服务：
    - 兼容旧字段（summary / published）
    - 同时补充规范字段（abstract / published_date）
    - 尽量少影响旧代码
    """

    def clean_paper(self, raw_paper: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(raw_paper, dict):
            raise ValueError("raw_paper 必须是字典。")

        paper_id = self._clean_text(raw_paper.get("paper_id", ""))
        source = self._clean_text(raw_paper.get("source", "arxiv")) or "arxiv"
        title = self._clean_text(raw_paper.get("title", ""))
        authors = self._clean_authors(raw_paper.get("authors", ""))

        # 兼容旧字段 summary 和新字段 abstract
        summary = self._clean_text(raw_paper.get("summary", raw_paper.get("abstract", "")))
        abstract = summary

        # 兼容旧字段 published 和新字段 published_date
        published = self._clean_date(raw_paper.get("published", raw_paper.get("published_date", "")))
        published_date = published

        updated = self._clean_date(raw_paper.get("updated", raw_paper.get("updated_date", "")))
        updated_date = updated

        link = self._clean_url(raw_paper.get("link", ""))
        pdf_url = self._clean_url(raw_paper.get("pdf_url", ""))
        pdf_local_path = self._clean_text(raw_paper.get("pdf_local_path", ""))

        # 若没有 paper_id，则尝试从 link 提取
        if not paper_id:
            paper_id = self._extract_paper_id_from_link(link)

        # 若没有 pdf_url，则尝试从 link 构造
        if not pdf_url and link:
            pdf_url = self._build_pdf_url_from_link(link)

        return {
            # 主键/来源
            "paper_id": paper_id,
            "source": source,

            # 原始核心字段（兼容旧代码）
            "title": title,
            "authors": authors,
            "summary": summary,
            "published": published,
            "updated": updated,
            "link": link,

            # 规范字段（兼容新代码）
            "abstract": abstract,
            "published_date": published_date,
            "updated_date": updated_date,
            "pdf_url": pdf_url,
            "pdf_local_path": pdf_local_path,
        }

    def clean_papers(self, raw_papers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not raw_papers:
            return []

        cleaned = []
        for item in raw_papers:
            try:
                paper = self.clean_paper(item)
                if paper.get("title") or paper.get("summary"):
                    cleaned.append(paper)
            except Exception:
                continue
        return cleaned

    def _clean_text(self, value: Any) -> str:
        if value is None:
            return ""
        text = str(value)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _clean_authors(self, authors: Any) -> str:
        if authors is None:
            return ""

        if isinstance(authors, list):
            result = []
            for item in authors:
                if isinstance(item, dict):
                    name = self._clean_text(item.get("name", ""))
                    if name:
                        result.append(name)
                else:
                    name = self._clean_text(item)
                    if name:
                        result.append(name)
            return ", ".join(result)

        return self._clean_text(authors)

    def _clean_date(self, value: Any) -> str:
        return self._clean_text(value)

    def _clean_url(self, value: Any) -> str:
        url = self._clean_text(value)
        if not url:
            return ""

        parsed = urlparse(url)
        if not parsed.scheme:
            return url
        return url

    def _extract_paper_id_from_link(self, link: str) -> str:
        """
        支持：
        - https://arxiv.org/abs/2501.12345
        - https://arxiv.org/abs/2501.12345v2
        - https://arxiv.org/pdf/2501.12345.pdf
        """
        if not link:
            return ""

        match = re.search(r"arxiv\.org/(?:abs|pdf)/([0-9]+\.[0-9]+(?:v\d+)?)", link)
        if match:
            return match.group(1)

        return ""

    def _build_pdf_url_from_link(self, link: str) -> str:
        if not link:
            return ""

        paper_id = self._extract_paper_id_from_link(link)
        if not paper_id:
            return ""

        return f"https://arxiv.org/pdf/{paper_id}.pdf"