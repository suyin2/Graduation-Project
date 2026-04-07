from services.analysis_service import AnalysisService


def main():
    keyword = input("请输入测试主题：").strip()
    if not keyword:
        print("输入不能为空。")
        return

    service = AnalysisService()

    try:
        result = service.run_analysis(
            keyword=keyword,
            save_report=False
        )
    except Exception as e:
        print("运行失败：", e)
        return

    query_info = result.get("query_info", {})
    topic_status = result.get("topic_status", {})
    need_fetch = result.get("need_fetch", False)
    fetch_plan = result.get("fetch_plan", {})
    ingest_stats = result.get("ingest_stats", {})
    post_fetch_still_not_enough = result.get("post_fetch_still_not_enough", False)

    print("\n====== 查询信息 ======\n")
    print("原始关键词:", result.get("keyword", keyword))
    print("标准中文主题:", query_info.get("topic_zh", ""))
    print("标准英文主题:", query_info.get("topic_en", ""))
    print("论文检索词:", query_info.get("paper_query", ""))
    print()

    print("\n====== 数据库初始状态 ======\n")
    print("当前数据库论文数:", topic_status.get("paper_count", 0))
    print("数据库最新论文日期:", topic_status.get("latest_published_date", "无"))
    print("是否达到基础阈值:", "是" if topic_status.get("enough", False) else "否")
    print("近 5 年年度分布:", topic_status.get("year_counts", {}))
    print("缺失年份:", topic_status.get("insufficient_years", []))
    print()

    print("\n====== 是否触发补抓 ======\n")
    print("need_fetch:", need_fetch)
    print()

    if need_fetch:
        print("\n====== 补抓计划 ======\n")
        print("抓取模式:", fetch_plan.get("mode", ""))
        print("缺失年份:", fetch_plan.get("insufficient_years", []))
        print("每年抓取上限:", fetch_plan.get("year_fetch_count", 0))
        print()

        print("\n====== 补抓结果 ======\n")
        print("成功年份/任务:", fetch_plan.get("success_years", []))
        print("失败年份/任务:", fetch_plan.get("failed_years", []))
        print("空结果年份/任务:", fetch_plan.get("empty_years", []))
        print()

        print("\n====== 入库结果 ======\n")
        print("新增:", ingest_stats.get("inserted_count", 0))
        print("跳过重复:", ingest_stats.get("skipped_count", 0))
        print("失败:", ingest_stats.get("failed_count", 0))
        print()

    print("\n====== 补抓后数据库状态 ======\n")
    print("补抓后是否仍不足:", "是" if post_fetch_still_not_enough else "否")

    final_topic_status = result.get("topic_status", {})
    print("最终数据库论文数:", final_topic_status.get("paper_count", 0))
    print("最终最新论文日期:", final_topic_status.get("latest_published_date", "无"))
    print("最终年度分布:", final_topic_status.get("year_counts", {}))
    print("最终缺失年份:", final_topic_status.get("insufficient_years", []))
    print()

    print("\n====== 最终简要报告 ======\n")
    print(result.get("report", ""))


if __name__ == "__main__":
    main()