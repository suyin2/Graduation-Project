def truncate_text(text: str, max_len: int = 300) -> str:
    """
    截断文本，避免 prompt 过长
    """
    if not text:
        return ""
    text = str(text).replace("\n", " ").strip()
    return text[:max_len] + ("..." if len(text) > max_len else "")


def format_papers(papers: list, max_items: int = 8) -> str:
    """
    将论文列表格式化为 prompt 中可用的文本
    兼容数据库字段与旧字段：
    - abstract / summary
    - published_date / published
    """
    if not papers:
        return "无相关论文信息。"

    lines = []
    for i, p in enumerate(papers[:max_items], 1):
        title = p.get("title", "")
        authors = p.get("authors", "")
        published = p.get("published_date", p.get("published", ""))
        abstract = p.get("abstract", p.get("summary", ""))
        main_topic = p.get("main_topic", "")
        sub_topic = p.get("sub_topic", "")
        link = p.get("link", "")

        lines.append(
            f"{i}. 标题：{title}\n"
            f"   作者：{authors}\n"
            f"   发布时间：{published}\n"
            f"   主分类：{main_topic}\n"
            f"   子分类：{sub_topic}\n"
            f"   摘要：{truncate_text(abstract, 300)}\n"
            f"   链接：{link}"
        )
    return "\n\n".join(lines)


def format_year_distribution(year_counts: dict) -> str:
    """
    格式化年度论文分布
    """
    if not year_counts:
        return "无年度分布信息。"

    items = sorted(year_counts.items(), key=lambda x: str(x[0]))
    return "\n".join([f"- {year}：{count} 篇" for year, count in items])


def format_hotspots(hotspots: list, max_items: int = 10) -> str:
    """
    格式化热点信息
    hotspots 示例：
    [
        {"name": "大语言模型", "count": 12, "type": "sub_topic"},
        {"name": "RAG", "count": 8, "type": "tag"}
    ]
    """
    if not hotspots:
        return "无明显热点信息。"

    lines = []
    for i, item in enumerate(hotspots[:max_items], 1):
        name = item.get("name", "")
        count = item.get("count", 0)
        item_type = item.get("type", "")
        lines.append(f"{i}. 热点名称：{name}；出现次数：{count}；类型：{item_type}")
    return "\n".join(lines)


def format_timeline(timeline_data: list, max_items: int = 10) -> str:
    """
    格式化时间线信息
    timeline_data 示例：
    [
        {
            "year": "2022",
            "topics": ["多模态", "视觉语言"],
            "summary": "该年开始出现……"
        }
    ]
    """
    if not timeline_data:
        return "无明显时间线信息。"

    lines = []
    for item in timeline_data[:max_items]:
        year = item.get("year", "")
        topics = item.get("topics", [])
        summary = item.get("summary", "")
        topic_text = "、".join(topics) if topics else "无"
        lines.append(
            f"{year}：\n"
            f"  代表技术/主题：{topic_text}\n"
            f"  概述：{summary}"
        )
    return "\n".join(lines)


def build_analysis_prompt(keyword: str, query_info: dict, report_data: dict) -> str:
    """
    基于数据库查询结果构造技术情报分析报告 prompt

    report_data 推荐包含：
    - overview
    - papers
    - latest_papers
    - year_counts
    - hotspots
    - latest_hotspots
    - timeline_data
    """
    topic_zh = query_info.get("topic_zh", keyword)
    topic_en = query_info.get("topic_en", keyword)
    paper_query = query_info.get("paper_query", keyword)

    papers = report_data.get("papers", [])
    latest_papers = report_data.get("latest_papers", [])
    year_counts = report_data.get("year_counts", {})
    hotspots = report_data.get("hotspots", [])
    latest_hotspots = report_data.get("latest_hotspots", [])
    timeline_data = report_data.get("timeline_data", [])

    paper_text = format_papers(papers, max_items=8)
    latest_paper_text = format_papers(latest_papers, max_items=5)
    year_dist_text = format_year_distribution(year_counts)
    hotspot_text = format_hotspots(hotspots, max_items=10)
    latest_hotspot_text = format_hotspots(latest_hotspots, max_items=8)
    timeline_text = format_timeline(timeline_data, max_items=10)

    prompt = f"""
你是一个技术情报分析助手。请基于数据库中整理出的论文信息，围绕指定技术主题生成一份结构清晰、重点明确的技术分析报告。

【用户原始关键词】
{keyword}

【标准中文主题】
{topic_zh}

【标准英文主题】
{topic_en}

【论文检索词】
{paper_query}

【数据库代表性论文】
{paper_text}

【数据库最新论文】
{latest_paper_text}

【近年论文年度分布】
{year_dist_text}

【整体技术热点】
{hotspot_text}

【最新技术热点】
{latest_hotspot_text}

【技术演进时间线素材】
{timeline_text}

请严格按照以下格式输出，不要添加多余说明：

【研究主题】
用1-2句话说明该技术主题的核心含义，以及其主要研究范围。

【数据库论文概况】
概括数据库中该主题论文的整体规模、时间分布特征，以及近年的活跃程度。

【技术热点识别】
基于论文主题、子方向、标签或高频技术点，总结当前最值得关注的主要技术热点，并说明热点集中在哪里。

【技术演进时间线】
按年份梳理该主题的技术演进过程，说明不同年份出现了哪些代表性技术、研究重点或阶段性变化。

【最新技术热点】
结合最新年份论文，重点分析当前最新、最活跃、最值得关注的技术方向。

【趋势总结】
综合以上内容，对该主题未来可能持续升温的方向、后续值得跟踪的技术点做简要总结。

要求：
1. 内容必须基于提供的数据库材料，不要脱离材料随意发挥。
2. 表达要正式、清晰，适合技术分析报告场景。
3. 如果某部分信息不足，可以如实概括，不要编造。
4. 输出应体现“数据库分析”视角，而不是简单论文摘要拼接。
5. 不要输出 Markdown 代码块。
"""
    return prompt.strip()