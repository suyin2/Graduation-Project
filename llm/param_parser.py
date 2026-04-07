import os
import json
import re
from openai import OpenAI


class ParamParser:
    """
    将用户自然语言需求解析为结构化参数
    输出字段：
    - keyword
    - paper_limit
    - news_limit
    - arxiv_sort_mode
    """

    ALLOWED_SORT_MODES = {"相关性", "最新", "最早"}

    def __init__(
        self,
        model: str = "deepseek-chat",
        base_url: str = "https://api.deepseek.com",
        api_key: str = None,
        default_paper_limit: int = 5,
        default_news_limit: int = 5,
        min_paper_limit: int = 1,
        max_paper_limit: int = 10,
        min_news_limit: int = 1,
        max_news_limit: int = 10,
        default_sort_mode: str = "最新"
    ):
        self.api_key = api_key or os.environ.get("DEEPSEEK_API_KEY")
        if not self.api_key:
            raise ValueError("没有检测到 DEEPSEEK_API_KEY，请先设置环境变量")

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=base_url
        )
        self.model = model

        self.default_paper_limit = default_paper_limit
        self.default_news_limit = default_news_limit
        self.min_paper_limit = min_paper_limit
        self.max_paper_limit = max_paper_limit
        self.min_news_limit = min_news_limit
        self.max_news_limit = max_news_limit
        self.default_sort_mode = default_sort_mode

    def _build_prompt(self, user_request: str) -> str:
        return f"""
你是一个技术情报系统的参数提取助手。
请从用户输入中提取分析需求，并严格输出 JSON，不要输出解释，不要输出 markdown 代码块。

需要提取的字段如下：
- keyword: 技术主题关键词
- paper_limit: 参考论文数量
- news_limit: 参考新闻数量
- arxiv_sort_mode: arXiv排序方式，只能是“相关性”、“最新”或“最早”

规则：
1. 如果用户没有明确说明论文数量，paper_limit 设为 {self.default_paper_limit}
2. 如果用户没有明确说明新闻数量，news_limit 设为 {self.default_news_limit}
3. 论文数量范围限制为 {self.min_paper_limit}-{self.max_paper_limit}
4. 新闻数量范围限制为 {self.min_news_limit}-{self.max_news_limit}
5. 如果用户没有明确说明排序方式，arxiv_sort_mode 设为 “{self.default_sort_mode}”
6. 如果用户说“按相关度”“按匹配度”，统一归为“相关性”
7. 如果用户说“按最新”“最新的”“最近的”，统一归为“最新”
8. 如果用户说“按最早”“最老”“早期的”，统一归为“最早”
9. keyword 必须尽量简洁，提取核心技术主题，不要把整句话都放进去

输出示例：
{{
  "keyword": "机器学习",
  "paper_limit": 5,
  "news_limit": 6,
  "arxiv_sort_mode": "最新"
}}

用户输入：
{user_request}
"""

    def _extract_json(self, text: str) -> dict:
        text = text.strip()

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            candidate = match.group(0)
            return json.loads(candidate)

        raise ValueError(f"模型返回内容不是合法 JSON：{text}")

    def _clamp(self, value: int, min_value: int, max_value: int, default: int) -> int:
        if not isinstance(value, int):
            return default
        if value < min_value:
            return min_value
        if value > max_value:
            return max_value
        return value

    def _normalize_sort_mode(self, sort_mode: str) -> str:
        if not sort_mode:
            return self.default_sort_mode

        sort_mode = str(sort_mode).strip()

        mapping = {
            "相关": "相关性",
            "相关性": "相关性",
            "相关度": "相关性",
            "匹配度": "相关性",
            "relevance": "相关性",

            "最新": "最新",
            "最近": "最新",
            "最新的": "最新",
            "最近的": "最新",
            "newest": "最新",
            "latest": "最新",

            "最早": "最早",
            "最老": "最早",
            "早期": "最早",
            "最早的": "最早",
            "oldest": "最早",
            "earliest": "最早",
        }

        normalized = mapping.get(sort_mode, sort_mode)
        if normalized not in self.ALLOWED_SORT_MODES:
            return self.default_sort_mode
        return normalized

    def _post_process(self, data: dict, user_request: str) -> dict:
        keyword = str(data.get("keyword", "")).strip()
        if not keyword:
            keyword = user_request.strip()

        paper_limit = data.get("paper_limit", self.default_paper_limit)
        news_limit = data.get("news_limit", self.default_news_limit)
        arxiv_sort_mode = self._normalize_sort_mode(data.get("arxiv_sort_mode", self.default_sort_mode))

        # 尝试把字符串数字转成 int
        try:
            paper_limit = int(paper_limit)
        except Exception:
            paper_limit = self.default_paper_limit

        try:
            news_limit = int(news_limit)
        except Exception:
            news_limit = self.default_news_limit

        paper_limit = self._clamp(
            paper_limit,
            self.min_paper_limit,
            self.max_paper_limit,
            self.default_paper_limit
        )
        news_limit = self._clamp(
            news_limit,
            self.min_news_limit,
            self.max_news_limit,
            self.default_news_limit
        )

        return {
            "keyword": keyword,
            "paper_limit": paper_limit,
            "news_limit": news_limit,
            "arxiv_sort_mode": arxiv_sort_mode
        }

    def parse(self, user_request: str) -> dict:
        user_request = user_request.strip()
        if not user_request:
            raise ValueError("用户输入不能为空")

        prompt = self._build_prompt(user_request)

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "你是一个严谨的参数提取助手。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            stream=False
        )

        content = response.choices[0].message.content
        data = self._extract_json(content)
        return self._post_process(data, user_request)
def parse_params(user_request: str) -> dict:
    parser = ParamParser()
    return parser.parse(user_request)