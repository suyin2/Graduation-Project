from front_end.begin_welcome import build_welcome_message
from services.analysis_service import AnalysisService


def run_analysis(
    keyword: str,
    save_report: bool = True,
    sort_mode: str = None,
    paper_limit: int = None
) -> dict:
    keyword = keyword.strip()
    if not keyword:
        raise ValueError("输入不能为空。")

    service = AnalysisService()

    result = service.run_analysis(
        keyword=keyword,
        save_report=save_report,
        sort_mode=sort_mode,
        paper_limit=paper_limit
    )

    result["welcome_message"] = build_welcome_message()
    return result