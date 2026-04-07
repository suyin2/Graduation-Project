import os
import re
from datetime import datetime


def sanitize_filename(name: str) -> str:
    """
    清理文件名中的非法字符
    """
    name = re.sub(r'[\\/:*?"<>|]', "_", name)
    name = name.strip()
    return name or "report"


def format_paper_refs(papers: list, max_items: int = 5) -> list:
    """
    格式化参考论文，只保留精简信息
    兼容：
    - published / published_date
    - summary / abstract
    """
    if not papers:
        return ["- 无相关论文\n"]

    lines = []
    for i, p in enumerate(papers[:max_items], 1):
        title = p.get("title", "")
        authors = p.get("authors", "")
        published = p.get("published_date", p.get("published", ""))
        link = p.get("link", "")
        main_topic = p.get("main_topic", "")
        sub_topic = p.get("sub_topic", "")

        lines.append(f"### {i}. {title}")
        lines.append(f"- 作者：{authors}")
        lines.append(f"- 发布时间：{published}")
        if main_topic:
            lines.append(f"- 主分类：{main_topic}")
        if sub_topic:
            lines.append(f"- 子分类：{sub_topic}")
        lines.append(f"- 链接：{link}\n")

    return lines


def format_hotspots(hotspots: list, max_items: int = 8) -> list:
    """
    格式化热点信息
    兼容规则热点与 LLM 热点：
    - name
    - count
    - type
    - reason
    """
    if not hotspots:
        return ["- 暂无明显热点信息。\n"]

    lines = []
    for i, item in enumerate(hotspots[:max_items], 1):
        name = item.get("name", "")
        item_type = item.get("type", "")
        count = item.get("count", "")
        reason = item.get("reason", "")

        text = f"- {i}. {name}"
        extras = []

        if item_type:
            extras.append(f"类型：{item_type}")
        if count not in ("", None):
            extras.append(f"频次：{count}")
        if reason:
            extras.append(f"说明：{reason}")

        if extras:
            text += f"（{'；'.join(str(x) for x in extras if x)}）"

        lines.append(text)

    lines.append("")
    return lines


def format_timeline_data(timeline_data: list, max_items: int = 10) -> list:
    """
    格式化时间线
    """
    if not timeline_data:
        return ["- 暂无时间线信息。\n"]

    lines = []
    for item in timeline_data[:max_items]:
        year = item.get("year", "")
        paper_count = item.get("paper_count", 0)
        topics = item.get("topics", [])
        summary = item.get("summary", "")

        lines.append(f"### {year}")
        lines.append(f"- 论文数量：{paper_count}")
        lines.append(f"- 研究方向：{'、'.join(topics) if topics else '暂无明显方向'}")
        if summary:
            lines.append(f"- 概述：{summary}")
        lines.append("")

    return lines


def build_markdown_report(
    keyword: str,
    query_info: dict,
    report_data: dict,
    report: str
) -> str:
    """
    将分析结果整理为更正式的 Markdown 文本
    """
    lines = []

    matched_main_topic = report_data.get("matched_main_topic", "")
    matched_sub_topic = report_data.get("matched_sub_topic", "")
    topic_status = report_data.get("topic_status", {}) or {}

    papers = report_data.get("papers", [])
    latest_papers = report_data.get("latest_papers", [])
    hotspots = report_data.get("hotspots", [])
    latest_hotspots = report_data.get("latest_hotspots", [])
    timeline_data = report_data.get("timeline_data", [])
    year_counts = report_data.get("year_counts", {})

    lines.append(f"# 技术情报分析报告：{keyword}\n")

    lines.append("## 一、基本信息\n")
    lines.append(f"- 原始关键词：{keyword}")
    lines.append(f"- 标准中文主题：{query_info.get('topic_zh', '')}")
    lines.append(f"- 标准英文主题：{query_info.get('topic_en', '')}")
    lines.append(f"- 论文检索词：{query_info.get('paper_query', '')}")
    if matched_main_topic:
        lines.append(f"- 命中主主题：{matched_main_topic}")
    if matched_sub_topic:
        lines.append(f"- 命中子主题：{matched_sub_topic}")
    if topic_status.get("latest_published_date"):
        lines.append(f"- 当前数据库最新论文日期：{topic_status.get('latest_published_date', '')}")
    lines.append(f"- 报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    lines.append("## 二、数据库概况\n")
    if year_counts:
        year_desc = "；".join([f"{year}：{count} 篇" for year, count in year_counts.items()])
        lines.append(f"- 近年论文分布：{year_desc}")
    else:
        lines.append("- 近年论文分布：暂无可用统计信息。")

    lines.append(f"- 代表性论文样本数：{len(papers)}")
    lines.append(f"- 最新论文样本数：{len(latest_papers)}")
    lines.append(f"- 整体热点数：{len(hotspots)}")
    lines.append(f"- 最新热点数：{len(latest_hotspots)}\n")

    lines.append("## 三、综合分析报告\n")
    lines.append(report.strip() + "\n")

    lines.append("## 四、整体技术热点\n")
    lines.extend(format_hotspots(hotspots, max_items=8))

    lines.append("## 五、最新技术热点\n")
    lines.extend(format_hotspots(latest_hotspots, max_items=6))

    lines.append("## 六、技术演进时间线\n")
    lines.extend(format_timeline_data(timeline_data, max_items=10))

    lines.append("## 七、代表性论文\n")
    lines.extend(format_paper_refs(papers, max_items=5))

    lines.append("## 八、最新论文\n")
    lines.extend(format_paper_refs(latest_papers, max_items=5))

    return "\n".join(lines)


def save_markdown_report(
    keyword: str,
    query_info: dict,
    report_data: dict,
    report: str,
    output_dir: str = "reports"
) -> str:
    """
    保存 Markdown 报告到本地文件
    :return: 保存后的文件路径
    """
    os.makedirs(output_dir, exist_ok=True)

    safe_keyword = sanitize_filename(keyword)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_keyword}_技术情报分析报告_{timestamp}.md"
    filepath = os.path.join(output_dir, filename)

    markdown_text = build_markdown_report(
        keyword=keyword,
        query_info=query_info,
        report_data=report_data,
        report=report
    )

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(markdown_text)

    return filepath