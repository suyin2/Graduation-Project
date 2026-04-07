from typing import Any, Dict, List, Tuple

from llm.param_parser import ParamParser
from llm.rewriter import QueryRewriter
from front_end.begin_welcome import build_welcome_message

from services.paper_fetch_service import PaperFetchService
from services.paper_ingest_service import PaperIngestService
from services.paper_query_service import PaperQueryService
from database.db import init_db
from llm.analyze import ReportAnalyzer

class AnalysisService:
    """
    后端总分析服务（当前阶段）：
    1. 解析参数
    2. 改写查询
    3. 先查数据库
    4. 如果数据库论文不足，则按缺口年份抓取并逐年入库
    5. 重新查数据库并复检
    6. 构建报告生成所需的 report_data
    7. 返回结构化结果
    """

    ANALYSIS_TARGET_COUNT = 15
    PER_YEAR_MIN_PAPER_COUNT = 2
    YEAR_FETCH_COUNT = 5

    def __init__(self):
        init_db()

        self.param_parser = ParamParser()
        self.rewriter = QueryRewriter()

        self.paper_query_service = PaperQueryService()
        self.paper_ingest_service = PaperIngestService()

    def run_analysis(
        self,
        keyword: str,
        sort_mode: str = None,
        paper_limit: int = None,
        save_report: bool = True
    ) -> Dict[str, Any]:
        keyword = (keyword or "").strip()
        if not keyword:
            raise ValueError("输入不能为空。")

        params = self.param_parser.parse(keyword)

        final_sort_mode = sort_mode or params.get("arxiv_sort_mode") or "最新"
        final_paper_limit = paper_limit or params.get("paper_limit") or 10
        final_paper_limit = max(1, min(50, int(final_paper_limit)))

        params["arxiv_sort_mode"] = final_sort_mode
        params["paper_limit"] = final_paper_limit
        params["analysis_target_count"] = self.ANALYSIS_TARGET_COUNT
        params["per_year_min_paper_count"] = self.PER_YEAR_MIN_PAPER_COUNT

        query_info = self._build_query_info(keyword)

        db_result = self._query_local_database(query_info, final_paper_limit)
        need_fetch = not db_result["enough"]

        fetched_papers: List[Dict[str, Any]] = []
        used_paper_query = ""
        ingest_stats: Dict[str, Any] = {
            "inserted_count": 0,
            "skipped_count": 0,
            "failed_count": 0
        }
        fetch_plan: Dict[str, Any] = {}

        if need_fetch:
            fetch_service = PaperFetchService(
                max_results=self.YEAR_FETCH_COUNT,
                sort_mode=final_sort_mode
            )

            fetched_papers, used_queries, fetch_plan, ingest_stats = self._fetch_by_gap_years(
                fetch_service=fetch_service,
                query_info=query_info,
                db_result=db_result,
                keyword=keyword
            )

            used_paper_query = " | ".join(used_queries) if used_queries else ""

        final_db_result = self._query_local_database(query_info, final_paper_limit)
        post_fetch_still_not_enough = not final_db_result.get("enough", False)

        # =========================
        # 新增：构建报告生成所需的结构化数据
        # =========================
        report_data = self._build_report_data(
            keyword=keyword,
            query_info=query_info,
            db_result=final_db_result
        )

        simple_report = self._build_simple_report(
            keyword=keyword,
            query_info=query_info,
            db_result=final_db_result,
            need_fetch=need_fetch,
            ingest_stats=ingest_stats,
            fetch_plan=fetch_plan,
            post_fetch_still_not_enough=post_fetch_still_not_enough
        )
        final_report = ""
        try:
            analyzer = ReportAnalyzer()
            final_report = analyzer.generate_report(
                keyword=keyword,
                query_info=query_info,
                report_data=report_data
            )
        except Exception as e:
            print(f"[WARN] 正式报告生成失败：{e}")
            final_report = ""
        save_path = None
        if save_report:
            save_path = None

        return {
            "welcome_message": build_welcome_message(),
            "keyword": keyword,
            "params": params,
            "query_info": query_info,

            "local_query_before_fetch": db_result,
            "need_fetch": need_fetch,
            "post_fetch_still_not_enough": post_fetch_still_not_enough,

            "fetch_plan": fetch_plan,
            "fetched_papers": fetched_papers,
            "used_paper_query": used_paper_query,
            "ingest_stats": ingest_stats,

            "papers": final_db_result.get("papers", []),
            "topic_status": final_db_result.get("topic_status", {}),
            "report_data": report_data,
            "report": simple_report,
            "final_report": final_report,
            "save_path": save_path,
            "news_list": [],
        }

    def _fetch_by_gap_years(
        self,
        fetch_service: PaperFetchService,
        query_info: Dict[str, Any],
        db_result: Dict[str, Any],
        keyword: str
    ) -> Tuple[List[Dict[str, Any]], List[str], Dict[str, Any], Dict[str, Any]]:
        topic_status = db_result.get("topic_status", {}) or {}
        insufficient_years = topic_status.get("insufficient_years", []) or []

        all_papers: List[Dict[str, Any]] = []
        used_queries: List[str] = []

        total_ingest_stats = {
            "inserted_count": 0,
            "skipped_count": 0,
            "failed_count": 0
        }

        fetch_plan = {
            "mode": "",
            "insufficient_years": insufficient_years,
            "year_fetch_count": self.YEAR_FETCH_COUNT,
            "success_years": [],
            "failed_years": [],
            "empty_years": [],
        }

        if insufficient_years:
            fetch_plan["mode"] = "gap_years"

            for year in insufficient_years:
                try:
                    year_papers, used_query = fetch_service.fetch_papers_by_year(
                        query_info=query_info,
                        year=int(year)
                    )

                    if used_query:
                        used_queries.append(f"{used_query}@{year}")

                    if not year_papers:
                        fetch_plan["empty_years"].append(year)
                        continue

                    year_papers = self._dedup_papers(year_papers)
                    if not year_papers:
                        fetch_plan["empty_years"].append(year)
                        continue

                    all_papers.extend(year_papers)

                    ingest_result = self.paper_ingest_service.ingest_papers(
                        raw_papers=year_papers,
                        topic=query_info.get("topic_zh", "") or query_info.get("topic_en", "") or keyword,
                        source="arxiv",
                        query_used=used_query or f"gap_year_{year}"
                    )

                    total_ingest_stats["inserted_count"] += ingest_result.get("inserted_count", 0)
                    total_ingest_stats["skipped_count"] += ingest_result.get("skipped_count", 0)
                    total_ingest_stats["failed_count"] += ingest_result.get("failed_count", 0)

                    fetch_plan["success_years"].append(year)

                except Exception as e:
                    print(f"[WARN] 按年份抓取或入库失败，year={year}：{e}")
                    fetch_plan["failed_years"].append(year)
                    continue

        else:
            fetch_plan["mode"] = "fallback_5y"

            try:
                fallback_papers, used_query = fetch_service.fetch_papers(
                    query_info=query_info,
                    use_default_5y=True
                )

                if used_query:
                    used_queries.append(used_query)

                fallback_papers = self._dedup_papers(fallback_papers)

                if fallback_papers:
                    all_papers.extend(fallback_papers)

                    ingest_result = self.paper_ingest_service.ingest_papers(
                        raw_papers=fallback_papers,
                        topic=query_info.get("topic_zh", "") or query_info.get("topic_en", "") or keyword,
                        source="arxiv",
                        query_used=used_query or "fallback_5y"
                    )

                    total_ingest_stats["inserted_count"] += ingest_result.get("inserted_count", 0)
                    total_ingest_stats["skipped_count"] += ingest_result.get("skipped_count", 0)
                    total_ingest_stats["failed_count"] += ingest_result.get("failed_count", 0)
                else:
                    fetch_plan["empty_years"].append("fallback_5y")

            except Exception as e:
                print(f"[WARN] fallback_5y 抓取或入库失败：{e}")
                fetch_plan["failed_years"].append("fallback_5y")

        deduped_papers = self._dedup_papers(all_papers)
        return deduped_papers, used_queries, fetch_plan, total_ingest_stats

    def _dedup_papers(self, papers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        deduped: List[Dict[str, Any]] = []
        seen_keys = set()

        for paper in papers:
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

        required_count = self.ANALYSIS_TARGET_COUNT

        if matched_main_topic:
            enough_result = self.paper_query_service.is_topic_data_enough(
                main_topic=matched_main_topic,
                min_paper_count=required_count,
                years=5,
                per_year_min_paper_count=self.PER_YEAR_MIN_PAPER_COUNT
            )
            enough = enough_result["enough"]
            topic_status = enough_result
            topic_status["required_count"] = required_count
        else:
            enough = len(papers) >= required_count
            topic_status = {
                "main_topic": matched_main_topic,
                "enough": enough,
                "paper_count": len(papers),
                "required_count": required_count,
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
        """
        基于最终数据库查询结果，构建给 LLM 报告生成使用的 report_data。
        """
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

    def _build_simple_report(
        self,
        keyword: str,
        query_info: Dict[str, Any],
        db_result: Dict[str, Any],
        need_fetch: bool,
        ingest_stats: Dict[str, Any],
        fetch_plan: Dict[str, Any],
        post_fetch_still_not_enough: bool = False
    ) -> str:
        papers = db_result.get("papers", [])
        topic_status = db_result.get("topic_status", {})
        matched_main_topic = db_result.get("matched_main_topic", "")
        matched_sub_topic = db_result.get("matched_sub_topic", "")

        display_topic = matched_main_topic or matched_sub_topic or query_info.get("topic_zh", "") or keyword

        lines = []
        lines.append(f"研究主题：{display_topic}")
        lines.append(f"当前查询返回论文数：{len(papers)}")

        latest_published_date = topic_status.get("latest_published_date", "")
        if latest_published_date:
            lines.append(f"当前数据库中该主题最新论文日期：{latest_published_date}")

        lines.append(f"当前基础分析阈值：{self.ANALYSIS_TARGET_COUNT} 篇")
        lines.append(f"近 5 年每年最低阈值：{self.PER_YEAR_MIN_PAPER_COUNT} 篇")

        year_counts = topic_status.get("year_counts", {})
        insufficient_years = topic_status.get("insufficient_years", [])

        if year_counts:
            year_desc = "，".join([f"{year}:{count}" for year, count in year_counts.items()])
            lines.append(f"近 5 年论文分布：{year_desc}")

        if insufficient_years:
            lines.append(f"当前缺口年份：{', '.join(str(y) for y in insufficient_years)}")

        if need_fetch:
            inserted_count = ingest_stats.get("inserted_count", 0)
            skipped_count = ingest_stats.get("skipped_count", 0)
            failed_count = ingest_stats.get("failed_count", 0)

            if fetch_plan.get("mode") == "gap_years":
                lines.append("本次数据库论文不足，已按缺口年份触发分年抓取与增量入库。")
            else:
                lines.append("本次数据库论文不足，已触发联网抓取与增量入库。")

            if fetch_plan.get("success_years"):
                lines.append(f"成功抓取并处理的年份：{', '.join(str(y) for y in fetch_plan['success_years'])}")

            if fetch_plan.get("failed_years"):
                lines.append(f"抓取或入库失败的年份/任务：{', '.join(str(y) for y in fetch_plan['failed_years'])}")

            if fetch_plan.get("empty_years"):
                lines.append(f"未抓到结果的年份/任务：{', '.join(str(y) for y in fetch_plan['empty_years'])}")

            lines.append(
                f"入库结果：新增 {inserted_count} 篇，跳过重复 {skipped_count} 篇，失败 {failed_count} 篇。"
            )

            if inserted_count == 0:
                lines.append("注意：本轮抓取未带来新增论文，说明抓到的结果可能大多已在库中或被判定为重复。")

            if post_fetch_still_not_enough:
                lines.append("抓取入库后，当前数据库论文数仍未达到基础分析阈值，后续仍需继续补抓。")
            else:
                lines.append("抓取入库后，当前数据库论文数已达到基础分析阈值。")
        else:
            lines.append("当前数据库中的论文数量与年份分布已满足基础分析要求，本次未触发联网抓取。")

        if len(papers) >= self.ANALYSIS_TARGET_COUNT:
            lines.append("当前已有较稳定的基础论文样本，可用于后续热点分析与时间线准备。")
        elif papers:
            lines.append("当前已有基础论文样本，但数量仍偏少，更适合做初步分析，后续建议继续补充。")
        else:
            lines.append("当前数据库中仍缺少可用论文，后续需继续补充数据。")

        return "\n".join(lines)

    def _contains_any(self, text: str, keywords: List[str]) -> bool:
        return any(keyword in text for keyword in keywords)


def run_analysis(
    keyword: str,
    sort_mode: str = None,
    paper_limit: int = None,
    save_report: bool = True
) -> Dict[str, Any]:
    service = AnalysisService()
    return service.run_analysis(
        keyword=keyword,
        sort_mode=sort_mode,
        paper_limit=paper_limit,
        save_report=save_report
    )