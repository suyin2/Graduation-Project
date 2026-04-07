from typing import Any, Dict, List


class PaperClassifyService:
    """
    通用论文分类服务（第一版）：
    - 不绑定某个具体专业
    - 先给出宽泛主领域
    - 再提取关键词标签
    - 对未知领域保持中性，不乱判
    """

    # 宽泛领域规则：适合第一版做主分类
    DOMAIN_RULES = {
        "人工智能与计算机": [
            "artificial intelligence", "machine learning", "deep learning",
            "neural network", "large language model", "llm", "computer vision",
            "natural language processing", "nlp", "retrieval", "agent",
            "multimodal", "code generation", "reasoning"
        ],
        "医学与生命科学": [
            "medical", "medicine", "clinical", "disease", "diagnosis",
            "patient", "biomedical", "biology", "genomics", "gene",
            "protein", "drug discovery", "healthcare"
        ],
        "材料与化学": [
            "material", "materials", "chemical", "chemistry", "catalyst",
            "molecule", "polymer", "nanomaterial", "electrolyte", "synthesis"
        ],
        "物理与天文": [
            "physics", "quantum", "particle", "cosmology", "astrophysics",
            "astronomy", "optics", "mechanics", "thermodynamics"
        ],
        "工程与控制": [
            "robot", "robotics", "control", "mechanical", "electrical",
            "signal processing", "embedded system", "manufacturing",
            "automation", "power system"
        ],
        "教育与社会科学": [
            "education", "learning science", "student", "teaching",
            "psychology", "sociology", "policy", "economics", "finance",
            "management", "behavior"
        ],
        "环境与地球科学": [
            "climate", "environment", "ecology", "geology", "earth science",
            "remote sensing", "weather", "carbon", "sustainability"
        ]
    }

    # 通用标签规则：先尽量提取可复用标签
    TAG_RULES = {
        "benchmark": ["benchmark", "evaluation", "eval", "assessment"],
        "survey": ["survey", "review", "overview", "systematic review"],
        "dataset": ["dataset", "corpus", "data collection"],
        "framework": ["framework", "pipeline", "system architecture", "architecture"],
        "optimization": ["optimization", "optimizing", "optimal"],
        "simulation": ["simulation", "simulator"],
        "prediction": ["prediction", "forecasting", "predictive"],
        "classification": ["classification", "classify", "classifier"],
        "generation": ["generation", "generative", "generate"],
        "retrieval": ["retrieval", "search", "information retrieval"],
        "efficiency": ["efficient", "efficiency", "acceleration", "speedup"],
        "experiment": ["experiment", "experimental"],
        "theory": ["theory", "theoretical", "proof"],
        "application": ["application", "applied", "real-world"],
        "medical": ["medical", "clinical", "patient", "healthcare"],
        "biology": ["biology", "biological", "genomics", "protein", "gene"],
        "education": ["education", "student", "teaching", "learning"],
        "robotics": ["robot", "robotics", "control", "automation"],
        "vision": ["image", "vision", "visual", "video"],
        "language": ["language", "text", "nlp", "dialogue"],
    }

    def classify_paper(self, paper: Dict[str, Any]) -> Dict[str, Any]:
        title = (paper.get("title", "") or "").lower()
        abstract = (paper.get("abstract", "") or "").lower()
        text = f"{title} {abstract}"

        main_topic = self._classify_main_topic(text)
        sub_topic = self._classify_sub_topic(text, main_topic)
        tags = self._extract_tags(text)

        contribution_summary = self._build_contribution_summary(paper)
        method_summary = self._build_method_summary(text)
        application_scenario = self._build_application_scenario(text)
        timeline_stage = ""

        categories = []
        if main_topic:
            categories.append({
                "category_type": "main_topic",
                "category_value": main_topic,
                "confidence": 0.65,
                "source_method": "rule",
            })

        if sub_topic:
            categories.append({
                "category_type": "sub_topic",
                "category_value": sub_topic,
                "confidence": 0.55,
                "source_method": "rule",
            })

        tag_items = [{"tag": tag, "tag_type": "keyword"} for tag in tags]

        return {
            "main_topic": main_topic,
            "sub_topic": sub_topic,
            "categories": categories,
            "tags": tag_items,
            "contribution_summary": contribution_summary,
            "method_summary": method_summary,
            "application_scenario": application_scenario,
            "timeline_stage": timeline_stage,
        }

    def classify_papers(self, papers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        for paper in papers:
            try:
                results.append(self.classify_paper(paper))
            except Exception:
                results.append({
                    "main_topic": "未分类领域",
                    "sub_topic": "",
                    "categories": [],
                    "tags": [],
                    "contribution_summary": "",
                    "method_summary": "",
                    "application_scenario": "",
                    "timeline_stage": "",
                })
        return results

    def _classify_main_topic(self, text: str) -> str:
        scores = {}

        for domain, keywords in self.DOMAIN_RULES.items():
            score = 0
            for keyword in keywords:
                if keyword in text:
                    score += 1
            if score > 0:
                scores[domain] = score

        if not scores:
            return "未分类领域"

        # 取命中次数最多的领域
        return max(scores, key=scores.get)

    def _classify_sub_topic(self, text: str, main_topic: str) -> str:
        """
        第一版子主题尽量保守，不做太细的硬判。
        更偏向给一个宽泛方向。
        """
        if main_topic == "人工智能与计算机":
            if self._contains_any(text, ["computer vision", "image", "video", "visual"]):
                return "视觉相关"
            if self._contains_any(text, ["natural language processing", "nlp", "language model", "text"]):
                return "语言相关"
            if self._contains_any(text, ["retrieval", "search", "information retrieval"]):
                return "检索相关"
            if self._contains_any(text, ["agent", "planning", "tool use"]):
                return "智能体相关"
            return "通用计算方向"

        if main_topic == "医学与生命科学":
            if self._contains_any(text, ["clinical", "patient", "diagnosis"]):
                return "临床相关"
            if self._contains_any(text, ["gene", "genomics", "protein"]):
                return "基因与蛋白相关"
            if self._contains_any(text, ["drug", "molecule"]):
                return "药物与分子相关"
            return "生命科学相关"

        if main_topic == "材料与化学":
            if self._contains_any(text, ["battery", "electrolyte", "energy storage"]):
                return "能源材料相关"
            if self._contains_any(text, ["catalyst", "catalysis"]):
                return "催化相关"
            return "材料化学相关"

        if main_topic == "物理与天文":
            if self._contains_any(text, ["quantum"]):
                return "量子相关"
            if self._contains_any(text, ["astronomy", "astrophysics", "cosmology"]):
                return "天文相关"
            return "物理相关"

        if main_topic == "工程与控制":
            if self._contains_any(text, ["robot", "robotics"]):
                return "机器人相关"
            if self._contains_any(text, ["control", "controller"]):
                return "控制相关"
            if self._contains_any(text, ["signal processing"]):
                return "信号处理相关"
            return "工程技术相关"

        if main_topic == "教育与社会科学":
            if self._contains_any(text, ["education", "student", "teaching"]):
                return "教育相关"
            if self._contains_any(text, ["economics", "finance"]):
                return "经济金融相关"
            if self._contains_any(text, ["psychology", "behavior"]):
                return "心理与行为相关"
            return "社会科学相关"

        if main_topic == "环境与地球科学":
            if self._contains_any(text, ["climate", "carbon"]):
                return "气候与碳相关"
            if self._contains_any(text, ["remote sensing", "satellite"]):
                return "遥感相关"
            return "环境地学相关"

        return ""

    def _extract_tags(self, text: str) -> List[str]:
        tags = []

        for tag, keywords in self.TAG_RULES.items():
            if self._contains_any(text, keywords):
                tags.append(tag)

        return tags

    def _build_contribution_summary(self, paper: Dict[str, Any]) -> str:
        title = paper.get("title", "") or ""
        if not title:
            return ""
        return f"该论文围绕“{title}”提出了相应的问题研究、方法或分析结果。"

    def _build_method_summary(self, text: str) -> str:
        if self._contains_any(text, ["framework", "architecture", "pipeline"]):
            return "论文主要提出了一种框架、流程或系统结构。"
        if self._contains_any(text, ["benchmark", "evaluation", "assessment"]):
            return "论文主要围绕评测体系、实验比较或指标分析展开。"
        if self._contains_any(text, ["model", "algorithm", "method"]):
            return "论文主要提出了一种模型、算法或方法并进行了验证。"
        if self._contains_any(text, ["survey", "review", "overview"]):
            return "论文主要对某一研究方向进行了综述与整理。"
        return "论文主要围绕某个研究问题展开，并给出了方法或分析。"

    def _build_application_scenario(self, text: str) -> str:
        if self._contains_any(text, ["medical", "clinical", "healthcare"]):
            return "医疗健康"
        if self._contains_any(text, ["education", "student", "teaching"]):
            return "教育"
        if self._contains_any(text, ["industry", "manufacturing", "automation"]):
            return "工业制造"
        if self._contains_any(text, ["finance", "economics", "trading"]):
            return "金融经济"
        if self._contains_any(text, ["climate", "environment", "ecology"]):
            return "环境"
        return ""

    def _contains_any(self, text: str, keywords: List[str]) -> bool:
        return any(keyword in text for keyword in keywords)