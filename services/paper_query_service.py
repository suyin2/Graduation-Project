from typing import Any, Dict, List
from datetime import datetime
from collections import Counter, defaultdict
import re

from database.paper_repository import PaperRepository
from database.category_repository import CategoryRepository
from database.tag_repository import TagRepository
from database.update_log_repository import UpdateLogRepository


class PaperQueryService:
    """
    论文数据库查询服务：
    - 按主题取论文
    - 按关键词搜索
    - 判断数据库中的论文是否足够
    - 获取某主题最近更新时间
    - 统计近 N 年的论文年度分布
    - 为报告生成提供热点、最新热点、时间线等结构化数据
    """

    # 用于从 title / abstract 中抽细粒度技术短语时做简单过滤
    HOTSPOT_STOPWORDS = {
        "a", "an", "the", "and", "or", "of", "for", "to", "in", "on", "with", "by",
        "from", "at", "as", "is", "are", "was", "were", "be", "been", "being",
        "this", "that", "these", "those", "into", "about", "over", "under", "via",
        "based", "using", "use", "used", "study", "studies", "paper", "research",
        "method", "methods", "model", "models", "approach", "approaches",
        "framework", "frameworks", "system", "systems", "analysis", "task", "tasks",
        "learning", "deep learning", "machine learning", "artificial intelligence"
    }

    HOTSPOT_BANNED_PHRASES = {
        "state of the art",
        "experimental results",
        "real world",
        "large scale",
        "novel method",
        "proposed method",
        "our method",
        "recent years",
        "different tasks",
        "various tasks",
        "neural network",
        "neural networks",
        "language model",
        "language models",
        "large language model",
        "large language models"
    }

    def __init__(self):
        self.paper_repo = PaperRepository()
        self.category_repo = CategoryRepository()
        self.tag_repo = TagRepository()
        self.update_log_repo = UpdateLogRepository()

    def get_papers_by_main_topic(
        self,
        main_topic: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        return self.paper_repo.get_papers_by_main_topic(main_topic=main_topic, limit=limit)

    def get_papers_by_sub_topic(
        self,
        sub_topic: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        return self.paper_repo.get_papers_by_sub_topic(sub_topic=sub_topic, limit=limit)

    def get_papers_by_category(
        self,
        category_type: str,
        category_value: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        return self.category_repo.get_papers_by_category(
            category_type=category_type,
            category_value=category_value,
            limit=limit
        )

    def get_papers_by_tag(
        self,
        tag: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        return self.tag_repo.get_papers_by_tag(tag=tag, limit=limit)

    def search_papers(
        self,
        keyword: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        return self.paper_repo.search_papers_by_keyword(keyword=keyword, limit=limit)

    def get_topic_status(self, main_topic: str) -> Dict[str, Any]:
        """
        查看某个主题在数据库中的基本状态：
        - 论文数量
        - 最新论文日期
        - 最近一次数据库更新时间
        """
        count = self.paper_repo.get_paper_count_by_topic(main_topic)
        latest_published_date = self.paper_repo.get_latest_published_date_by_topic(main_topic)
        latest_update_time = self.update_log_repo.get_latest_update_time_by_topic(main_topic)

        return {
            "main_topic": main_topic,
            "paper_count": count,
            "latest_published_date": latest_published_date,
            "latest_update_time": latest_update_time,
        }

    def get_year_distribution_by_main_topic(
        self,
        main_topic: str,
        years: int = 5,
        per_year_min_paper_count: int = 2,
        fetch_limit: int = 300
    ) -> Dict[str, Any]:
        """
        统计某个主主题近 N 年的论文年度分布。
        """
        current_year = datetime.now().year
        target_years = [current_year - i for i in range(years - 1, -1, -1)]

        papers = self.get_papers_by_main_topic(main_topic=main_topic, limit=fetch_limit)

        year_counts = {year: 0 for year in target_years}

        for paper in papers:
            published_text = (
                paper.get("published_date")
                or paper.get("published")
                or ""
            )
            year = self._extract_year(published_text)

            if year in year_counts:
                year_counts[year] += 1

        insufficient_years = [
            year for year, count in year_counts.items()
            if count < per_year_min_paper_count
        ]

        return {
            "main_topic": main_topic,
            "years": years,
            "target_years": target_years,
            "year_counts": year_counts,
            "per_year_min_paper_count": per_year_min_paper_count,
            "insufficient_years": insufficient_years,
            "year_distribution_enough": len(insufficient_years) == 0,
        }

    def is_topic_data_enough(
        self,
        main_topic: str,
        min_paper_count: int = 15,
        years: int = 5,
        per_year_min_paper_count: int = 2,
        fetch_limit_for_distribution: int = 300
    ) -> Dict[str, Any]:
        """
        判断数据库中某主题的论文是否足够：
        1. 总量是否达到 min_paper_count
        2. 近 years 年中，每年是否至少有 per_year_min_paper_count 篇
        """
        status = self.get_topic_status(main_topic)
        total_enough = status["paper_count"] >= min_paper_count

        distribution_result = self.get_year_distribution_by_main_topic(
            main_topic=main_topic,
            years=years,
            per_year_min_paper_count=per_year_min_paper_count,
            fetch_limit=fetch_limit_for_distribution
        )

        year_distribution_enough = distribution_result["year_distribution_enough"]
        enough = total_enough and year_distribution_enough

        return {
            "main_topic": main_topic,
            "enough": enough,

            "paper_count": status["paper_count"],
            "required_count": min_paper_count,

            "latest_published_date": status["latest_published_date"],
            "latest_update_time": status["latest_update_time"],

            "years": years,
            "per_year_min_paper_count": per_year_min_paper_count,
            "year_counts": distribution_result["year_counts"],
            "insufficient_years": distribution_result["insufficient_years"],
            "total_enough": total_enough,
            "year_distribution_enough": year_distribution_enough,
        }

    def get_recent_papers(self, limit: int = 20) -> List[Dict[str, Any]]:
        return self.paper_repo.list_recent_papers(limit=limit)

    def get_related_papers(
        self,
        query_text: str,
        main_topic: str = "",
        sub_topic: str = "",
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        统一入口：
        1. 优先按 main_topic 查
        2. 再按 sub_topic 查
        3. 最后按关键词搜索
        """
        if main_topic:
            papers = self.get_papers_by_main_topic(main_topic, limit=limit)
            if papers:
                return papers

        if sub_topic:
            papers = self.get_papers_by_sub_topic(sub_topic, limit=limit)
            if papers:
                return papers

        return self.search_papers(query_text, limit=limit)

    def get_related_papers_by_candidates(
        self,
        query_text: str,
        main_topic_candidates: List[str],
        sub_topic_candidates: List[str],
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        按候选主题逐个尝试：
        - 哪个主主题先命中，就用哪个
        - 再尝试子主题
        - 最后回退到关键词搜索
        """
        for topic in main_topic_candidates:
            topic = (topic or "").strip()
            if not topic:
                continue

            papers = self.get_papers_by_main_topic(topic, limit=limit)
            if papers:
                return {
                    "papers": papers,
                    "matched_main_topic": topic,
                    "matched_sub_topic": "",
                    "match_type": "main_topic",
                }

        for sub_topic in sub_topic_candidates:
            sub_topic = (sub_topic or "").strip()
            if not sub_topic:
                continue

            papers = self.get_papers_by_sub_topic(sub_topic, limit=limit)
            if papers:
                return {
                    "papers": papers,
                    "matched_main_topic": "",
                    "matched_sub_topic": sub_topic,
                    "match_type": "sub_topic",
                }

        papers = self.search_papers(query_text, limit=limit)
        return {
            "papers": papers,
            "matched_main_topic": "",
            "matched_sub_topic": "",
            "match_type": "keyword",
        }

    # =========================
    # 以下为“报告生成专用查询”
    # =========================

    def get_representative_papers(
        self,
        query_text: str,
        main_topic: str = "",
        sub_topic: str = "",
        limit: int = 8,
        fetch_limit: int = 100,
        years: int = 5
    ) -> List[Dict[str, Any]]:
        """
        获取代表性论文：
        规则：
        1. 先取主题相关论文作为主体样本
        2. 再按近几年做时间覆盖，每年尽量补 1 篇
        3. 最后去重并截断
        """
        papers = self.get_related_papers(
            query_text=query_text,
            main_topic=main_topic,
            sub_topic=sub_topic,
            limit=fetch_limit
        )

        if not papers:
            return []

        papers_sorted = sorted(
            papers,
            key=lambda x: str(x.get("published_date") or x.get("published") or ""),
            reverse=True
        )

        selected = []
        seen_keys = set()

        head_count = max(1, limit // 2)
        for p in papers_sorted[:head_count]:
            key = p.get("paper_id") or p.get("link") or p.get("title", "").strip().lower()
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            selected.append(p)

        current_year = datetime.now().year
        target_years = [current_year - i for i in range(years - 1, -1, -1)]

        for year in reversed(target_years):
            for p in papers_sorted:
                published_text = p.get("published_date") or p.get("published") or ""
                paper_year = self._extract_year(published_text)
                if paper_year != year:
                    continue

                key = p.get("paper_id") or p.get("link") or p.get("title", "").strip().lower()
                if not key or key in seen_keys:
                    continue

                seen_keys.add(key)
                selected.append(p)
                break

            if len(selected) >= limit:
                break

        return selected[:limit]

    def get_latest_papers(
        self,
        query_text: str,
        main_topic: str = "",
        sub_topic: str = "",
        limit: int = 5,
        fetch_limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        获取最新论文：
        当前阶段先在服务层按 published_date 排序。
        """
        papers = self.get_related_papers(
            query_text=query_text,
            main_topic=main_topic,
            sub_topic=sub_topic,
            limit=fetch_limit
        )

        papers = sorted(
            papers,
            key=lambda x: str(
                x.get("published_date")
                or x.get("published")
                or ""
            ),
            reverse=True
        )
        return papers[:limit]

    def get_hotspots(
        self,
        query_text: str,
        main_topic: str = "",
        sub_topic: str = "",
        fetch_limit: int = 200,
        top_k: int = 10
    ) -> List[Dict[str, Any]]:
        """
        识别整体热点：
        当前仍以 sub_topic / main_topic 做粗粒度统计。
        """
        papers = self.get_related_papers(
            query_text=query_text,
            main_topic=main_topic,
            sub_topic=sub_topic,
            limit=fetch_limit
        )

        counter = Counter()

        for paper in papers:
            sub_topic_value = str(paper.get("sub_topic", "") or "").strip()
            main_topic_value = str(paper.get("main_topic", "") or "").strip()

            if sub_topic_value:
                counter[("sub_topic", sub_topic_value)] += 1
            elif main_topic_value:
                counter[("main_topic", main_topic_value)] += 1

        hotspots = []
        for (item_type, name), count in counter.most_common(top_k):
            hotspots.append({
                "name": name,
                "count": count,
                "type": item_type,
            })

        return hotspots

    def get_latest_hotspots(
        self,
        query_text: str,
        main_topic: str = "",
        sub_topic: str = "",
        latest_years: int = 2,
        fetch_limit: int = 200,
        top_k: int = 8
    ) -> List[Dict[str, Any]]:
        """
        识别最新热点：
        只统计最近 latest_years 年的论文。
        """
        current_year = datetime.now().year
        valid_years = set(current_year - i for i in range(latest_years))

        papers = self.get_related_papers(
            query_text=query_text,
            main_topic=main_topic,
            sub_topic=sub_topic,
            limit=fetch_limit
        )

        counter = Counter()

        for paper in papers:
            published_text = (
                paper.get("published_date")
                or paper.get("published")
                or ""
            )
            year = self._extract_year(published_text)
            if year not in valid_years:
                continue

            sub_topic_value = str(paper.get("sub_topic", "") or "").strip()
            main_topic_value = str(paper.get("main_topic", "") or "").strip()

            if sub_topic_value:
                counter[("sub_topic", sub_topic_value)] += 1
            elif main_topic_value:
                counter[("main_topic", main_topic_value)] += 1

        hotspots = []
        for (item_type, name), count in counter.most_common(top_k):
            hotspots.append({
                "name": name,
                "count": count,
                "type": item_type,
            })

        return hotspots

    def get_timeline_data(
        self,
        query_text: str,
        main_topic: str = "",
        sub_topic: str = "",
        years: int = 5,
        fetch_limit: int = 300,
        top_k_per_year: int = 4
    ) -> List[Dict[str, Any]]:
        """
        构造技术时间线素材（细化版）：
        - 按年份聚合
        - 降低粗 sub_topic 的影响
        - 更依赖标题/摘要中的细粒度技术短语
        - 每年补充 1~2 篇代表性论文标题，增强说明
        """
        current_year = datetime.now().year
        target_years = [current_year - i for i in range(years - 1, -1, -1)]

        papers = self.get_related_papers(
            query_text=query_text,
            main_topic=main_topic,
            sub_topic=sub_topic,
            limit=fetch_limit
        )

        papers_by_year = defaultdict(list)

        for paper in papers:
            published_text = (
                paper.get("published_date")
                or paper.get("published")
                or ""
            )
            year = self._extract_year(published_text)
            if year in target_years:
                papers_by_year[year].append(paper)

        timeline_data = []

        for year in target_years:
            year_papers = papers_by_year.get(year, [])
            paper_count = len(year_papers)

            if paper_count == 0:
                timeline_data.append({
                    "year": str(year),
                    "paper_count": 0,
                    "topics": [],
                    "summary": "该年数据库中相关论文较少，暂未形成明显技术热点。"
                })
                continue

            fine_topics = self._extract_year_fine_grained_topics(
                papers=year_papers,
                top_k=top_k_per_year
            )

            representative_titles = self._get_year_representative_papers(
                papers=year_papers,
                limit=2
            )
            rep_title_text = "；".join(
                [p.get("title", "") for p in representative_titles if p.get("title", "")]
            )

            if fine_topics:
                summary = (
                    f"该年相关论文约 {paper_count} 篇，"
                    f"研究重点更集中在：{'、'.join(fine_topics)}。"
                )
            else:
                summary = (
                    f"该年相关论文约 {paper_count} 篇，"
                    f"但数据库中尚未提炼出足够明确的细粒度研究方向。"
                )

            if rep_title_text:
                summary += f" 代表性论文包括：{rep_title_text}。"

            timeline_data.append({
                "year": str(year),
                "paper_count": paper_count,
                "topics": fine_topics,
                "summary": summary,
            })

        return timeline_data

    def build_report_data(
        self,
        query_text: str,
        main_topic: str = "",
        sub_topic: str = "",
        years: int = 5,
        representative_limit: int = 8,
        latest_limit: int = 5,
        fetch_limit: int = 300
    ) -> Dict[str, Any]:
        """
        一次性组装报告生成需要的数据。
        """
        year_distribution = {}
        if main_topic:
            year_distribution = self.get_year_distribution_by_main_topic(
                main_topic=main_topic,
                years=years,
                per_year_min_paper_count=0,
                fetch_limit=fetch_limit
            ).get("year_counts", {})
        else:
            papers_for_year = self.get_related_papers(
                query_text=query_text,
                main_topic=main_topic,
                sub_topic=sub_topic,
                limit=fetch_limit
            )

            current_year = datetime.now().year
            target_years = [current_year - i for i in range(years - 1, -1, -1)]
            year_distribution = {year: 0 for year in target_years}

            for paper in papers_for_year:
                published_text = (
                    paper.get("published_date")
                    or paper.get("published")
                    or ""
                )
                year = self._extract_year(published_text)
                if year in year_distribution:
                    year_distribution[year] += 1

        representative_papers = self.get_representative_papers(
            query_text=query_text,
            main_topic=main_topic,
            sub_topic=sub_topic,
            limit=representative_limit
        )

        latest_papers = self.get_latest_papers(
            query_text=query_text,
            main_topic=main_topic,
            sub_topic=sub_topic,
            limit=latest_limit,
            fetch_limit=fetch_limit
        )

        hotspots = self.get_hotspots(
            query_text=query_text,
            main_topic=main_topic,
            sub_topic=sub_topic,
            fetch_limit=fetch_limit,
            top_k=10
        )

        latest_hotspots = self.get_latest_hotspots(
            query_text=query_text,
            main_topic=main_topic,
            sub_topic=sub_topic,
            latest_years=2,
            fetch_limit=fetch_limit,
            top_k=8
        )

        timeline_data = self.get_timeline_data(
            query_text=query_text,
            main_topic=main_topic,
            sub_topic=sub_topic,
            years=years,
            fetch_limit=fetch_limit,
            top_k_per_year=4
        )

        return {
            "papers": representative_papers,
            "latest_papers": latest_papers,
            "year_counts": year_distribution,
            "hotspots": hotspots,
            "latest_hotspots": latest_hotspots,
            "timeline_data": timeline_data,
        }

    # =========================
    # 时间线 / 细粒度短语辅助方法
    # =========================

    def _get_year_representative_papers(
        self,
        papers: List[Dict[str, Any]],
        limit: int = 2
    ) -> List[Dict[str, Any]]:
        """
        从某一年的论文中选少量代表性论文：
        - 先按时间降序
        - 再优先保留标题/摘要信息较完整的论文
        """
        sorted_papers = sorted(
            papers,
            key=lambda x: str(x.get("published_date") or x.get("published") or ""),
            reverse=True
        )

        selected = []
        seen_keys = set()

        for p in sorted_papers:
            title = str(p.get("title", "") or "").strip()
            abstract = str(p.get("abstract") or p.get("summary") or "").strip()
            key = p.get("paper_id") or p.get("link") or title.lower()

            if not key or key in seen_keys:
                continue
            if not title:
                continue

            if len(title) < 8 and len(abstract) < 40:
                continue

            seen_keys.add(key)
            selected.append(p)

            if len(selected) >= limit:
                break

        return selected

    def _extract_year_fine_grained_topics(
        self,
        papers: List[Dict[str, Any]],
        top_k: int = 4
    ) -> List[str]:
        """
        从某一年的论文中提取更细粒度研究方向：
        - 降低宽泛 sub_topic 的影响
        - 更依赖 title + abstract 的技术短语
        """
        counter = Counter()

        broad_subtopics = {
            "视觉相关", "语言相关", "检索相关", "智能体相关",
            "通用计算方向", "生命科学相关", "材料化学相关",
            "物理相关", "工程技术相关", "社会科学相关", "环境地学相关",
            "临床相关", "教育相关", "遥感相关", "量子相关"
        }

        for paper in papers:
            sub_topic_value = str(paper.get("sub_topic", "") or "").strip()

            # 如果子分类本身不宽，就给一点权重；太宽的直接弱化
            if sub_topic_value and sub_topic_value not in broad_subtopics:
                counter[sub_topic_value] += 1

            text = self._merge_text_for_phrase_extraction(paper)
            phrases = self._extract_candidate_phrases(text)

            for phrase in phrases:
                counter[phrase] += 2

        topics = []
        for name, _ in counter.most_common(top_k * 3):
            if name in broad_subtopics:
                continue
            if len(name) < 6:
                continue
            topics.append(name)
            if len(topics) >= top_k:
                break

        return topics

    def _merge_text_for_phrase_extraction(self, paper: Dict[str, Any]) -> str:
        title = str(paper.get("title", "") or "")
        abstract = str(
            paper.get("abstract")
            or paper.get("summary")
            or ""
        )
        return f"{title}. {abstract}"

    def _extract_candidate_phrases(self, text: str) -> List[str]:
        """
        轻量级技术短语提取：
        - 从英文 title / abstract 中提取 2~3 词短语
        - 过滤停用词、过泛词、无意义短语
        """
        if not text:
            return []

        text = text.lower()
        text = re.sub(r"[^a-z0-9\s\-]", " ", text)
        text = re.sub(r"\s+", " ", text).strip()

        words = text.split()
        words = [w for w in words if len(w) >= 3 and not w.isdigit()]

        candidates = []

        for n in [2, 3]:
            for i in range(len(words) - n + 1):
                phrase_words = words[i:i + n]

                if any(w in self.HOTSPOT_STOPWORDS for w in phrase_words):
                    continue

                phrase = " ".join(phrase_words).strip()

                if not phrase:
                    continue
                if phrase in self.HOTSPOT_STOPWORDS:
                    continue
                if phrase in self.HOTSPOT_BANNED_PHRASES:
                    continue
                if len(phrase) < 6:
                    continue

                if phrase.endswith(("study", "method", "methods", "model", "models", "approach", "approaches")):
                    continue

                candidates.append(phrase)

        deduped = []
        seen = set()
        for item in candidates:
            if item not in seen:
                seen.add(item)
                deduped.append(item)

        return deduped

    def _extract_year(self, published_text: str) -> int:
        """
        从发布日期字符串中提取年份。
        兼容：
        - 2026-03-24T15:54:14Z
        - 2026-03-24
        - 2026
        """
        text = str(published_text or "").strip()
        if len(text) >= 4 and text[:4].isdigit():
            return int(text[:4])
        return 0