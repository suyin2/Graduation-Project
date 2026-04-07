import os
import json
import re
from openai import OpenAI


class QueryRewriter:
    """
    使用大模型对用户输入的技术关键词进行查询改写：
    1. 生成适合 arXiv 的英文检索词
    2. 生成适合中文新闻搜索的中文检索词
    3. 输出统一结构，便于主程序调用
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

    def _build_prompt(self, user_keyword: str) -> str:
        return f"""
你是一个技术情报检索助手。用户会输入一个技术主题关键词或一段自然语言，请你将它改写为适合“arXiv论文检索”和“中文新闻检索”的查询表达。

要求：
1. 保持主题语义一致，不要偏题。
2. arXiv 检索词必须优先使用英文、简洁、学术风格的关键词表达。
3. 中文新闻检索词必须优先使用中文，可以适度补充常见技术术语。
4. 如果用户输入本身很模糊，例如“machine”“model”“vision”，请尽量推断其在 AI 语境下最常见的技术含义。
5. 输出必须是 JSON，不要输出任何解释、注释、代码块标记。
6. JSON 字段必须包含：
   - topic_zh
   - topic_en
   - paper_query
   - news_query
   - related_terms

用户输入关键词：{user_keyword}
"""

    def _extract_json(self, text: str) -> dict:
        """
        尽量从模型输出中提取 JSON
        """
        text = text.strip()

        # 直接尝试整体解析
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # 尝试提取第一个 {...}
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            candidate = match.group(0)
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass

        raise ValueError(f"模型返回内容不是合法 JSON：{text}")

    def rewrite(self, user_keyword: str) -> dict:
        prompt = self._build_prompt(user_keyword)

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "你是一个严谨的技术检索查询改写助手。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            stream=False
        )

        content = response.choices[0].message.content
        data = self._extract_json(content)

        # 做兜底清洗，避免缺字段
        result = {
            "topic_zh": data.get("topic_zh", user_keyword),
            "topic_en": data.get("topic_en", user_keyword),
            "paper_query": data.get("paper_query", user_keyword),
            "news_query": data.get("news_query", user_keyword),
            "related_terms": data.get("related_terms", [user_keyword])
        }

        if not isinstance(result["related_terms"], list):
            result["related_terms"] = [str(result["related_terms"])]

        return result