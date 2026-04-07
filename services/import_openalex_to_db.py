import os
from pprint import pprint

from services.openalex_import_service import OpenAlexImportService


def main():
    """
    直接运行示例：
    python import_openalex_to_db.py
    """

    # 可选：去 OpenAlex 后台申请免费 key 后填到环境变量里
    # Windows PowerShell:
    # $env:OPENALEX_API_KEY="你的key"
    # $env:OPENALEX_MAILTO="你的邮箱"
    api_key = os.getenv("OPENALEX_API_KEY", "").strip()
    mailto = os.getenv("OPENALEX_MAILTO", "").strip()

    service = OpenAlexImportService(
        per_page=25,
        api_key=api_key,
        mailto=mailto
    )

    result = service.import_by_keyword(
        keyword="machine learning",
        topic="机器学习",
        start_year=2023,
        end_year=2024,
        max_results=20,
        is_oa_only=False
    )

    print("=" * 80)
    print("OpenAlex 导入完成")
    print("=" * 80)
    pprint(result)


if __name__ == "__main__":
    main()