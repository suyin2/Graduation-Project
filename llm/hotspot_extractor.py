import json
import os
from typing import Any, Dict, List

from openai import OpenAI


def truncate_text(text: str, max_len: int = 400) -> str:
    if not text:
        return ""
    text = str(text).replace("\n", " ").strip()
    return text[:max_len] + ("..." if len(text) > max_len else "")


def format_papers_for_hotspot(papers: List[Dict[str, Any]], max_items: int = 12) -> str:
    if not papers:
        return "无相关论文信息。"

    lines = []
    for i, p in enumerate(papers[:max_items], 1):
        title = p.get("title", "")
        authors = p.get("authors", "")
        published = p.get("published_date", p.get("published", ""))
        abstract = p.get("abstract", p.get("summary", ""))
        sub_topic = p.get("sub_topic", "")
        main_topic = p.get("main_topic", "")
        lines.append(
            f"{i}. 标题：{title}\n"
            f"   作者：{authors}\n"
            f"   发布时间：{published}\n"
            f"   主分类：{main_topic}\n"
            f"   子分类：{sub_topic}\n"
            f"   摘要：{truncate_text(abstract, 400)}"
        )
    return "\n\n".join(lines)


class HotspotExtractor:
    """
    从论文标题/摘要中提取：
    1. 整体技术热点
    2. 最新技术热点

    输出尽量为结构化 JSON，便于接入 report_data
    """

    def __init__(
        self,
        model: str = "deepseek-chat",
        base_url: str = "https://api.deepseek.com",
        api_key: str = None
    ):
        self.api_key = api_key or os.environ.get("DEEPSEEK_API_KEY")
        if not self.api_key:
            raise ValueError("没有检测到 DEEPSEEK_API_KEY，请先设置环境变量")

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=base_url
        )
        self.model = model

    def extract_hotspots(
        self,
        keyword: str,
        papers: List[Dict[str, Any]],
        top_k: int = 8
    ) -> List[Dict[str, Any]]:
        """
        从一组论文中提取整体热点
        """
        prompt = self._build_hotspot_prompt(
            keyword=keyword,
            papers=papers,
            top_k=top_k,
            mode="overall"
        )
        return self._call_and_parse_json(prompt)

    def extract_latest_hotspots(
        self,
        keyword: str,
        papers: List[Dict[str, Any]],
        top_k: int = 6
    ) -> List[Dict[str, Any]]:
        """
        从最新论文中提取最新热点
        """
        prompt = self._build_hotspot_prompt(
            keyword=keyword,
            papers=papers,
            top_k=top_k,
            mode="latest"
        )
        return self._call_and_parse_json(prompt)

    def _build_hotspot_prompt(
        self,
        keyword: str,
        papers: List[Dict[str, Any]],
        top_k: int,
        mode: str = "overall"
    ) -> str:
        paper_text = format_papers_for_hotspot(papers, max_items=12)

        if mode == "latest":
            instruction = (
                "请从这些最新论文的标题和摘要中，提炼当前最新、最活跃、最值得关注的技术热点。"
                "重点识别近期新出现或明显升温的方向。"
            )
        else:
            instruction = (
                "请从这些论文的标题和摘要中，提炼整体技术热点。"
                "重点识别在多篇论文中反复出现、具有明确技术含义的研究方向、方法、任务或范式。"
            )

        return f"""
你是一个技术热点提取助手。

用户主题：{keyword}

下面给出一组论文的标题、子主题和摘要，请不要泛泛而谈，不要只输出“大模型、人工智能、机器学习”这类过宽泛的词。
请尽量提炼更具体的技术方向、方法名称、研究任务、系统范式或关键术语。

【论文材料】
{paper_text}

任务要求：
1. {instruction}
2. 优先提炼细粒度热点，例如某种方法、任务、架构、范式，而不是过大的学科名。
3. 如果多个词意思接近，请合并为一个更规范的热点名称。
4. 每个热点都要给出简短理由，说明它为什么算热点。
5. 输出 {top_k} 个以内的热点即可。
6. 必须输出 JSON 数组，且不要输出任何额外说明。

输出格式示例：
[
  {{
    "name": "retrieval augmented generation",
    "reason": "多篇论文围绕检索增强生成、外部知识接入和问答能力展开",
    "confidence": 0.87
  }},
  {{
    "name": "instruction tuning",
    "reason": "多篇论文关注模型对齐、指令跟随与任务泛化能力提升",
    "confidence": 0.82
  }}
]
""".strip()

    def _call_and_parse_json(self, prompt: str) -> List[Dict[str, Any]]:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你是一个严谨的技术热点提取助手。"
                        "你必须按要求输出合法 JSON 数组。"
                    )
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            stream=False
        )

        content = response.choices[0].message.content.strip()

        # 尽量兼容模型偶尔包裹 ```json ... ```
        content = content.replace("```json", "").replace("```", "").strip()

        try:
            data = json.loads(content)
            if isinstance(data, list):
                normalized = []
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    normalized.append({
                        "name": str(item.get("name", "")).strip(),
                        "reason": str(item.get("reason", "")).strip(),
                        "confidence": float(item.get("confidence", 0) or 0),
                        "type": "llm_hotspot"
                    })
                return [x for x in normalized if x["name"]]
        except Exception:
            pass

        # 如果模型没按要求返回 JSON，做兜底
        return [{
            "name": "热点提取失败",
            "reason": content[:500],
            "confidence": 0.0,
            "type": "llm_hotspot_fallback"
        }]