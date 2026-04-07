import os
from openai import OpenAI


class PaperSummarizer:
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

    def summarize(self, paper: dict) -> str:
        title = paper.get("title", "")
        authors = paper.get("authors", "")
        published = paper.get("published", "")
        summary = paper.get("summary", "")
        link = paper.get("link", "")

        prompt = f"""
请你根据以下论文信息，用中文做一个简洁清晰的论文总结。

要求：
1. 说明这篇论文主要研究什么问题。
2. 概括它使用了什么方法或思路。
3. 说明它的主要价值或意义。
4. 最后补一句：这篇论文适合什么人阅读。
5. 不要编造摘要中没有明确提到的实验结果或细节。
6. 输出尽量分点，语言自然，不要太长。

论文标题：{title}
作者：{authors}
发布时间：{published}
摘要：{summary}
链接：{link}
"""

        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "你是一个擅长阅读和总结学术论文的 AI 助手。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3
        )

        return resp.choices[0].message.content.strip()