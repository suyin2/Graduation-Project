from typing import Any, Dict, List

from database.db import init_db
from llm.param_parser import ParamParser
from llm.rewriter import QueryRewriter
from llm.analyze import ReportAnalyzer
from llm.hotspot_extractor import HotspotExtractor
from services.paper_query_service import PaperQueryService


class DBOnlyAnalysisRunner:
    """
    纯数据库分析入口：
    1. 解析参数
    2. 改写查询
    3. 只查询数据库，不触发爬虫
    4. 构建 report_data
    5. 使用大模型从摘要中提取热点
    6. 调用大模型生成最终报告
    """

    def __init__(self):
        init_db()
        self.param_parser = ParamParser()
        self.rewriter = QueryRewriter()
        self.paper_query_service = PaperQueryService()
        self.report_analyzer = ReportAnalyzer()
        self.hotspot_extractor = HotspotExtractor()

    def run(self, keyword: str, paper_limit: int = 10) -> Dict[str, Any]:
        keyword = (keyword or "").strip()
        if not keyword:
            raise ValueError("输入不能为空。")

        params = self.param_parser.parse(keyword)
        final_paper_limit = paper_limit or params.get("paper_limit") or 10
        final_paper_limit = max(1, min(50, int(final_paper_limit)))
        params["paper_limit"] = final_paper_limit

        query_info = self._build_query_info(keyword)

        db_result = self._query_local_database(query_info, final_paper_limit)

        report_data = self._build_report_data(
            keyword=keyword,
            query_info=query_info,
            db_result=db_result
        )

        # =========================
        # 新增：使用大模型从摘要中提取热点
        # =========================
        report_data = self._enhance_hotspots_with_llm(
            keyword=keyword,
            report_data=report_data
        )

        final_report = self.report_analyzer.generate_report(
            keyword=keyword,
            query_info=query_info,
            report_data=report_data
        )

        return {
            "keyword": keyword,
            "params": params,
            "query_info": query_info,
            "db_result": db_result,
            "report_data": report_data,
            "final_report": final_report,
        }

    def _build_query_info(self, keyword: str) -> Dict[str, Any]:
        try:
            query_info = self.rewriter.rewrite(keyword)
            if isinstance(query_info, dict):
                return query_info
        except Exception:
            pass

        return {
            "topic_zh": keyword,
            "topic_en": keyword,
            "paper_query": keyword,
            "news_query": keyword,
            "related_terms": [keyword],
        }

    def _query_local_database(
        self,
        query_info: Dict[str, Any],
        limit: int
    ) -> Dict[str, Any]:
        query_text = query_info.get("topic_zh") or query_info.get("topic_en") or query_info.get("paper_query", "")

        main_topic_candidates = self._guess_main_topic_candidates(query_info)
        sub_topic_candidates = self._guess_sub_topic_candidates(query_info)

        query_result = self.paper_query_service.get_related_papers_by_candidates(
            query_text=query_text,
            main_topic_candidates=main_topic_candidates,
            sub_topic_candidates=sub_topic_candidates,
            limit=limit
        )

        papers = query_result.get("papers", [])
        matched_main_topic = query_result.get("matched_main_topic", "")
        matched_sub_topic = query_result.get("matched_sub_topic", "")

        topic_status = {}
        enough = False

        if matched_main_topic:
            topic_status = self.paper_query_service.is_topic_data_enough(
                main_topic=matched_main_topic,
                min_paper_count=15,
                years=5,
                per_year_min_paper_count=2
            )
            enough = topic_status.get("enough", False)
        else:
            enough = len(papers) >= 15
            topic_status = {
                "main_topic": matched_main_topic,
                "enough": enough,
                "paper_count": len(papers),
                "required_count": 15,
                "latest_published_date": papers[0].get("published_date", papers[0].get("published", "")) if papers else "",
                "latest_update_time": "",
                "insufficient_years": [],
                "year_counts": {},
            }

        return {
            "papers": papers,
            "main_topic_candidates": main_topic_candidates,
            "sub_topic_candidates": sub_topic_candidates,
            "matched_main_topic": matched_main_topic,
            "matched_sub_topic": matched_sub_topic,
            "match_type": query_result.get("match_type", ""),
            "topic_status": topic_status,
            "enough": enough,
        }

    def _build_report_data(
        self,
        keyword: str,
        query_info: Dict[str, Any],
        db_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        query_text = query_info.get("topic_zh") or query_info.get("topic_en") or query_info.get("paper_query", keyword)
        matched_main_topic = db_result.get("matched_main_topic", "") or ""
        matched_sub_topic = db_result.get("matched_sub_topic", "") or ""

        report_data = self.paper_query_service.build_report_data(
            query_text=query_text,
            main_topic=matched_main_topic,
            sub_topic=matched_sub_topic,
            years=5,
            representative_limit=8,
            latest_limit=5,
            fetch_limit=300
        )

        report_data["matched_main_topic"] = matched_main_topic
        report_data["matched_sub_topic"] = matched_sub_topic
        report_data["topic_status"] = db_result.get("topic_status", {})

        return report_data

    def _enhance_hotspots_with_llm(
        self,
        keyword: str,
        report_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        用大模型替换原先规则统计得到的热点：
        - hotspots: 基于代表性论文提取整体热点
        - latest_hotspots: 基于最新论文提取最新热点
        """
        papers = report_data.get("papers", []) or []
        latest_papers = report_data.get("latest_papers", []) or []

        try:
            if papers:
                llm_hotspots = self.hotspot_extractor.extract_hotspots(
                    keyword=keyword,
                    papers=papers,
                    top_k=8
                )
                if llm_hotspots:
                    report_data["hotspots"] = llm_hotspots
        except Exception as e:
            print(f"[WARN] 整体热点提取失败：{e}")

        try:
            if latest_papers:
                llm_latest_hotspots = self.hotspot_extractor.extract_latest_hotspots(
                    keyword=keyword,
                    papers=latest_papers,
                    top_k=6
                )
                if llm_latest_hotspots:
                    report_data["latest_hotspots"] = llm_latest_hotspots
        except Exception as e:
            print(f"[WARN] 最新热点提取失败：{e}")

        return report_data

    def _guess_main_topic_candidates(self, query_info: Dict[str, Any]) -> List[str]:
        texts = [
            str(query_info.get("topic_zh", "") or "").lower(),
            str(query_info.get("topic_en", "") or "").lower(),
            str(query_info.get("paper_query", "") or "").lower(),
        ]
        full_text = " ".join(texts)

        candidates = []

        if self._contains_any(full_text, [
            "artificial intelligence", "machine learning", "deep learning",
            "llm", "large language model", "nlp", "computer vision",
            "retrieval", "agent", "multimodal", "reasoning"
        ]):
            candidates.append("人工智能与计算机")

        if self._contains_any(full_text, [
            "medical", "medicine", "clinical", "biology", "genomics",
            "gene", "protein", "drug", "healthcare"
        ]):
            candidates.append("医学与生命科学")

        if self._contains_any(full_text, [
            "material", "materials", "chemical", "chemistry",
            "molecule", "polymer", "catalyst"
        ]):
            candidates.append("材料与化学")

        if self._contains_any(full_text, [
            "physics", "quantum", "astronomy", "astrophysics", "cosmology"
        ]):
            candidates.append("物理与天文")

        if self._contains_any(full_text, [
            "robot", "robotics", "control", "automation",
            "signal processing", "manufacturing"
        ]):
            candidates.append("工程与控制")

        if self._contains_any(full_text, [
            "education", "student", "teaching", "economics",
            "finance", "psychology", "behavior"
        ]):
            candidates.append("教育与社会科学")

        if self._contains_any(full_text, [
            "climate", "environment", "ecology", "earth science",
            "remote sensing", "weather", "carbon"
        ]):
            candidates.append("环境与地球科学")

        topic_zh = (query_info.get("topic_zh", "") or "").strip()
        topic_en = (query_info.get("topic_en", "") or "").strip()

        if topic_zh and topic_zh not in candidates:
            candidates.append(topic_zh)
        if topic_en and topic_en not in candidates:
            candidates.append(topic_en)

        return candidates

    def _guess_sub_topic_candidates(self, query_info: Dict[str, Any]) -> List[str]:
        candidates = []

        related_terms = query_info.get("related_terms", [])
        if isinstance(related_terms, list):
            for term in related_terms:
                term = str(term).strip()
                if term and term not in candidates:
                    candidates.append(term)

        topic_zh = (query_info.get("topic_zh", "") or "").strip()
        topic_en = (query_info.get("topic_en", "") or "").strip()
        paper_query = (query_info.get("paper_query", "") or "").strip()

        for item in [topic_zh, topic_en, paper_query]:
            if item and item not in candidates:
                candidates.append(item)

        return candidates

    def _contains_any(self, text: str, keywords: List[str]) -> bool:
        return any(keyword in text for keyword in keywords)


if __name__ == "__main__":
    runner = DBOnlyAnalysisRunner()

    result = runner.run(
        keyword="machine learning",
        paper_limit=10
    )

    print("=" * 80)
    print("query_info:")
    print(result["query_info"])

    print("=" * 80)
    print("db_result:")
    print(result["db_result"])

    print("=" * 80)
    print("report_data keys:")
    print(result["report_data"].keys())

    print("=" * 80)
    print("LLM hotspots:")
    print(result["report_data"].get("hotspots", []))

    print("=" * 80)
    print("LLM latest_hotspots:")
    print(result["report_data"].get("latest_hotspots", []))

    print("=" * 80)
    print("final_report:")
    print(result["final_report"])