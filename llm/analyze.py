import os
from openai import OpenAI
from llm.prompt_built import build_analysis_prompt


class ReportAnalyzer:
    """
    技术情报分析器：
    1. 根据关键词、查询改写结果、数据库报告数据构造 prompt
    2. 调用大模型生成最终分析报告
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

    def generate_report(
        self,
        keyword: str,
        query_info: dict,
        report_data: dict
    ) -> str:
        """
        基于数据库结构化数据生成技术分析报告
        """
        prompt = build_analysis_prompt(
            keyword=keyword,
            query_info=query_info,
            report_data=report_data
        )

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你是一个严谨、克制、擅长技术情报总结的分析助手。"
                        "你需要基于数据库整理出的结构化论文信息，"
                        "输出正式、清晰、有条理的技术分析报告。"
                    )
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            stream=False
        )

        return response.choices[0].message.content.strip()