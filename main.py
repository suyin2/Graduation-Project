from front_end.analysis import run_analysis
from front_end.begin_welcome import build_welcome_message
from llm.analyze import ReportAnalyzer
from llm.reporter import save_markdown_report


def main():
    print(build_welcome_message())
    keyword = input("请输入你的分析需求：").strip()

    if not keyword:
        print("输入不能为空。")
        return

    try:
        # 完整主链：
        # 1. 先查数据库
        # 2. 若不足则按缺失年份补抓
        # 3. 返回最终 report_data
        result = run_analysis(keyword, save_report=False)
    except Exception as e:
        print("运行失败：", e)
        return

    query_info = result.get("query_info", {})
    topic_status = result.get("topic_status", {})
    report_data = result.get("report_data", {})
    ingest_stats = result.get("ingest_stats", {})
    fetch_plan = result.get("fetch_plan", {})
    need_fetch = result.get("need_fetch", False)
    post_fetch_still_not_enough = result.get("post_fetch_still_not_enough", False)

    papers = report_data.get("papers", [])
    latest_papers = report_data.get("latest_papers", [])
    hotspots = report_data.get("hotspots", [])
    latest_hotspots = report_data.get("latest_hotspots", [])
    timeline_data = report_data.get("timeline_data", [])
    year_counts = report_data.get("year_counts", {})

    print("\n====== 解析结果 ======\n")
    print("原始关键词:", result.get("keyword", keyword))
    print("标准中文主题:", query_info.get("topic_zh", ""))
    print("标准英文主题:", query_info.get("topic_en", ""))
    print("论文检索词:", query_info.get("paper_query", ""))
    related_terms = query_info.get("related_terms", [])
    print("相关术语:", ", ".join(related_terms) if related_terms else "无")
    print()

    print("\n====== 数据库初始状态 ======\n")
    print("当前数据库论文数:", topic_status.get("paper_count", len(result.get("papers", []))))
    print("数据库最新论文日期:", topic_status.get("latest_published_date", "无"))
    print("当前是否达到基础阈值:", "是" if topic_status.get("enough", False) else "否")
    print("近 5 年年度分布:", topic_status.get("year_counts", {}))
    print("缺失年份:", topic_status.get("insufficient_years", []))
    print()

    print("\n====== 是否触发补抓 ======\n")
    print("need_fetch:", need_fetch)
    print()

    if need_fetch:
        print("\n====== 补抓计划与结果 ======\n")
        print("抓取模式:", fetch_plan.get("mode", ""))
        print("缺失年份:", fetch_plan.get("insufficient_years", []))
        print("每年抓取上限:", fetch_plan.get("year_fetch_count", 0))
        print("成功年份/任务:", fetch_plan.get("success_years", []))
        print("失败年份/任务:", fetch_plan.get("failed_years", []))
        print("空结果年份/任务:", fetch_plan.get("empty_years", []))
        print()

        print("入库结果：")
        print("新增:", ingest_stats.get("inserted_count", 0))
        print("跳过重复:", ingest_stats.get("skipped_count", 0))
        print("失败:", ingest_stats.get("failed_count", 0))
        print()

    print("\n====== 补抓后数据库状态 ======\n")
    print("补抓后是否仍不足:", "是" if post_fetch_still_not_enough else "否")
    print()

    print("\n====== 年度分布 ======\n")
    if year_counts:
        for year, count in year_counts.items():
            print(f"{year}: {count} 篇")
    else:
        print("暂无年度分布信息。")
    print()

    print("\n====== 整体技术热点 ======\n")
    if hotspots:
        for i, item in enumerate(hotspots, 1):
            print(f"{i}. {item.get('name', '')}")
            if item.get("reason"):
                print("   说明:", item.get("reason", ""))
            else:
                print(
                    "   信息:",
                    f"type={item.get('type', '')}, count={item.get('count', '')}"
                )
    else:
        print("暂无整体热点信息。")
    print()

    print("\n====== 最新技术热点 ======\n")
    if latest_hotspots:
        for i, item in enumerate(latest_hotspots, 1):
            print(f"{i}. {item.get('name', '')}")
            if item.get("reason"):
                print("   说明:", item.get("reason", ""))
            else:
                print(
                    "   信息:",
                    f"type={item.get('type', '')}, count={item.get('count', '')}"
                )
    else:
        print("暂无最新热点信息。")
    print()

    print("\n====== 技术演进时间线 ======\n")
    if timeline_data:
        for item in timeline_data:
            print(f"{item.get('year', '')}:")
            print("  论文数量:", item.get("paper_count", 0))
            print("  研究方向:", "、".join(item.get("topics", [])) if item.get("topics") else "暂无明显方向")
            print("  概述:", item.get("summary", ""))
            print()
    else:
        print("暂无时间线信息。")
        print()

    print("\n====== 代表性论文（节选） ======\n")
    if papers:
        for p in papers[:5]:
            print("标题:", p.get("title", ""))
            print("作者:", p.get("authors", ""))
            print("发布时间:", p.get("published_date", p.get("published", "")))
            print("主分类:", p.get("main_topic", ""))
            print("子分类:", p.get("sub_topic", ""))
            print("链接:", p.get("link", ""))
            print()
    else:
        print("暂无代表性论文。")
        print()

    print("\n====== 最新论文（节选） ======\n")
    if latest_papers:
        for p in latest_papers[:5]:
            print("标题:", p.get("title", ""))
            print("作者:", p.get("authors", ""))
            print("发布时间:", p.get("published_date", p.get("published", "")))
            print("主分类:", p.get("main_topic", ""))
            print("子分类:", p.get("sub_topic", ""))
            print("链接:", p.get("link", ""))
            print()
    else:
        print("暂无最新论文。")
        print()

    try:
        analyzer = ReportAnalyzer()
        final_report = analyzer.generate_report(
            keyword=result.get("keyword", keyword),
            query_info=query_info,
            report_data=report_data
        )
    except Exception as e:
        print("报告生成失败：", e)
        return

    print("\n====== 最终分析报告 ======\n")
    print(final_report)
    print()

    try:
        save_path = save_markdown_report(
            keyword=result.get("keyword", keyword),
            query_info=query_info,
            report_data=report_data,
            report=final_report
        )
        print("Markdown 报告已保存到：", save_path)
    except Exception as e:
        print("报告保存失败：", e)


if __name__ == "__main__":
    main()