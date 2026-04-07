import streamlit as st
from front_end.analysis import run_analysis
from services.paper_query_service import PaperQueryService
from llm.rewriter import QueryRewriter
from front_end.analysis import AnalysisService
st.set_page_config(
    page_title="技术情报分析助手",
    page_icon="📘",
    layout="wide"
)

# =========================
# Session State 初始化
# =========================
if "analysis_result" not in st.session_state:
    st.session_state.analysis_result = None

if "analysis_error" not in st.session_state:
    st.session_state.analysis_error = None
if "db_query_result" not in st.session_state:
    st.session_state.db_query_result = None

if "db_query_error" not in st.session_state:
    st.session_state.db_query_error = None
# =========================
# 页面样式
# =========================
st.markdown("""
<style>
.main {
    background-color: #f6f8fb;
}
.block-container {
    padding-top: 2rem;
    padding-bottom: 2rem;
    max-width: 1250px;
}
.top-banner {
    background: linear-gradient(135deg, #eef4ff 0%, #f8fbff 100%);
    padding: 26px 30px;
    border-radius: 18px;
    border: 1px solid #dbe7ff;
    margin-bottom: 20px;
}
.top-banner h1 {
    margin: 0;
    color: #1f2937;
}
.top-banner p {
    margin-top: 8px;
    margin-bottom: 0;
    color: #6b7280;
    font-size: 0.96rem;
}
.section-card {
    background: #ffffff;
    padding: 20px 22px;
    border-radius: 18px;
    border: 1px solid #eaeef5;
    box-shadow: 0 4px 14px rgba(15, 23, 42, 0.04);
    margin-bottom: 16px;
}
.metric-card {
    background: #ffffff;
    padding: 16px 18px;
    border-radius: 16px;
    border: 1px solid #eaeef5;
    box-shadow: 0 4px 14px rgba(15, 23, 42, 0.04);
}
.metric-title {
    color: #6b7280;
    font-size: 0.9rem;
    margin-bottom: 6px;
}
.metric-value {
    color: #111827;
    font-size: 1.08rem;
    font-weight: 700;
}
.paper-card {
    background: #ffffff;
    padding: 18px 20px;
    border-radius: 16px;
    border: 1px solid #eaeef5;
    box-shadow: 0 4px 14px rgba(15, 23, 42, 0.04);
    margin-bottom: 14px;
}
.paper-title {
    font-size: 1.02rem;
    font-weight: 700;
    color: #111827;
    margin-bottom: 10px;
}
.meta-text {
    color: #4b5563;
    font-size: 0.94rem;
    margin-bottom: 6px;
}
.small-muted {
    color: #6b7280;
    font-size: 0.92rem;
}
.report-box {
    background: #ffffff;
    padding: 22px;
    border-radius: 18px;
    border: 1px solid #eaeef5;
    box-shadow: 0 4px 14px rgba(15, 23, 42, 0.04);
    line-height: 1.8;
}
.tag-chip {
    display: inline-block;
    padding: 4px 10px;
    margin: 4px 8px 4px 0;
    background: #eef4ff;
    border: 1px solid #dbe7ff;
    color: #3157a4;
    border-radius: 999px;
    font-size: 0.85rem;
}
.timeline-item {
    background: #ffffff;
    padding: 16px 18px;
    border-radius: 14px;
    border: 1px solid #eaeef5;
    margin-bottom: 12px;
}
.placeholder-box {
    background: #ffffff;
    padding: 24px;
    border-radius: 18px;
    border: 1px dashed #cfd8e3;
    color: #6b7280;
}
hr {
    border: none;
    border-top: 1px solid #edf2f7;
    margin: 1rem 0 1.2rem 0;
}
</style>
""", unsafe_allow_html=True)

# =========================
# 工具函数
# =========================
def safe_text(value, default="暂无"):
    if value is None:
        return default
    value = str(value).strip()
    return value if value else default


def render_metric_card(title: str, value: str):
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">{title}</div>
            <div class="metric-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_paper_card(index: int, paper: dict):
    title = safe_text(paper.get("title"), "无标题")
    authors = paper.get("authors", "未知")
    if isinstance(authors, list):
        authors = ", ".join(authors)

    published = (
        paper.get("published_date")
        or paper.get("published")
        or "未知"
    )
    main_topic = safe_text(paper.get("main_topic"), "未分类")
    sub_topic = safe_text(paper.get("sub_topic"), "未分类")
    abstract = paper.get("abstract") or paper.get("summary") or "暂无摘要"
    link = paper.get("link", "")

    st.markdown(
        f"""
        <div class="paper-card">
            <div class="paper-title">{index}. {title}</div>
            <div class="meta-text"><b>作者：</b>{authors}</div>
            <div class="meta-text"><b>发布时间：</b>{published}</div>
            <div class="meta-text"><b>主分类：</b>{main_topic}</div>
            <div class="meta-text"><b>子分类：</b>{sub_topic}</div>
            <div class="meta-text" style="margin-top:10px;"><b>摘要：</b>{abstract}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

    if link:
        st.markdown(f"[查看原文]({link})")


def render_hotspot_list(title: str, hotspots: list):
    st.markdown(f"#### {title}")
    if not hotspots:
        st.info("暂无相关热点信息。")
        return

    for i, item in enumerate(hotspots, 1):
        name = safe_text(item.get("name"), "未命名热点")
        reason = item.get("reason", "")
        count = item.get("count", "")
        item_type = item.get("type", "")

        with st.container():
            st.markdown(f"**{i}. {name}**")
            if reason:
                st.write(reason)
            else:
                extra = []
                if item_type:
                    extra.append(f"type={item_type}")
                if count != "":
                    extra.append(f"count={count}")
                if extra:
                    st.caption(" / ".join(extra))


def render_timeline(timeline_data: list):
    if not timeline_data:
        st.info("暂无时间线信息。")
        return

    for item in timeline_data:
        year = safe_text(item.get("year"), "未知年份")
        paper_count = item.get("paper_count", 0)
        topics = item.get("topics", []) or []
        summary = safe_text(item.get("summary"), "暂无概述")

        topic_html = ""
        if topics:
            topic_html = "".join([f'<span class="tag-chip">{t}</span>' for t in topics])

        st.markdown(
            f"""
            <div class="timeline-item">
                <div style="font-weight:700;font-size:1rem;margin-bottom:6px;">{year}</div>
                <div class="meta-text"><b>论文数量：</b>{paper_count}</div>
                <div class="meta-text"><b>研究方向：</b></div>
                <div style="margin:6px 0 8px 0;">{topic_html if topic_html else '<span class="small-muted">暂无明显方向</span>'}</div>
                <div class="meta-text"><b>概述：</b>{summary}</div>
            </div>
            """,
            unsafe_allow_html=True
        )


def render_year_distribution(year_counts: dict):
    if not year_counts:
        st.info("暂无年度分布信息。")
        return

    sorted_items = sorted(year_counts.items(), key=lambda x: str(x[0]))
    cols = st.columns(min(5, len(sorted_items)) if sorted_items else 1)

    for idx, (year, count) in enumerate(sorted_items):
        with cols[idx % len(cols)]:
            render_metric_card(str(year), f"{count} 篇")


# =========================
# 顶部标题
# =========================
st.markdown("""
<div class="top-banner">
    <h1>📘 技术跟踪情报分析助手</h1>
    <p>
        输入你想分析的技术主题，系统将优先基于数据库进行分析，
        在数据不足时按缺失年份补抓论文，并生成正式技术情报报告。
    </p>
</div>
""", unsafe_allow_html=True)

# =========================
# 主页面结构
# =========================
tab_report, tab_database = st.tabs(["报告分析", "数据库管理"])

# =========================
# Tab 1: 报告分析
# =========================
with tab_report:
    st.markdown("### 研究主题输入")

    user_input = st.text_area(
        "请输入你的分析需求",
        placeholder="例如：请分析多模态大模型近五年的技术进展、热点变化与演进趋势",
        height=130
    )

    col_btn_1, col_btn_2 = st.columns([1, 5])
    with col_btn_1:
        start_button = st.button("开始分析", use_container_width=True)

    if start_button:
        if not user_input.strip():
            st.warning("先把分析主题输进去。")
            st.session_state.analysis_result = None
            st.session_state.analysis_error = None
        else:
            try:
                with st.spinner("正在执行数据库分析与报告生成..."):
                    result = run_analysis(
                        user_input.strip(),
                        save_report=False
                    )
                st.session_state.analysis_result = result
                st.session_state.analysis_error = None
            except Exception as e:
                st.session_state.analysis_result = None
                st.session_state.analysis_error = str(e)

    if st.session_state.analysis_error:
        st.error(f"运行失败：{st.session_state.analysis_error}")

    if st.session_state.analysis_result is not None:
        result = st.session_state.analysis_result

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
        report_text = result.get("report", "")
        final_report_text = result.get("final_report", "")

        st.markdown("---")

        # 概览
        st.markdown("### 本次分析概览")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            render_metric_card("研究主题", safe_text(query_info.get("topic_zh") or result.get("keyword")))
        with c2:
            render_metric_card("数据库论文数", str(topic_status.get("paper_count", 0)))
        with c3:
            render_metric_card("是否触发补抓", "是" if need_fetch else "否")
        with c4:
            render_metric_card("补抓后是否仍不足", "是" if post_fetch_still_not_enough else "否")

        st.markdown("")

        # 分页展示
        subtab1, subtab2, subtab3, subtab4, subtab5, subtab6 = st.tabs(
            ["主题与状态", "补抓情况", "正式报告", "热点分析", "时间线", "论文样本"]
        )

        # 主题与状态
        with subtab1:
            st.markdown("#### 查询主题信息")

            info_col1, info_col2 = st.columns(2)
            with info_col1:
                st.markdown(f"**标准中文主题：** {safe_text(query_info.get('topic_zh'))}")
                st.markdown(f"**标准英文主题：** {safe_text(query_info.get('topic_en'))}")
                st.markdown(f"**论文检索词：** {safe_text(query_info.get('paper_query'))}")

            with info_col2:
                st.markdown(f"**命中主分类：** {safe_text(report_data.get('matched_main_topic'))}")
                st.markdown(f"**命中子分类：** {safe_text(report_data.get('matched_sub_topic'))}")
                st.markdown(f"**数据库最新论文日期：** {safe_text(topic_status.get('latest_published_date'))}")

            related_terms = query_info.get("related_terms", []) or []
            st.markdown("#### 相关术语")
            if related_terms:
                chips = "".join([f'<span class="tag-chip">{safe_text(term)}</span>' for term in related_terms])
                st.markdown(chips, unsafe_allow_html=True)
            else:
                st.info("暂无相关术语。")

            st.markdown("#### 近五年年度分布")
            render_year_distribution(year_counts)

            st.markdown("#### 缺失年份")
            insufficient_years = topic_status.get("insufficient_years", []) or []
            if insufficient_years:
                st.warning("缺失年份：" + "、".join([str(y) for y in insufficient_years]))
            else:
                st.success("近五年分布已满足基础要求。")

        # 补抓情况
        with subtab2:
            st.markdown("#### 补抓状态")
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                render_metric_card("新增", str(ingest_stats.get("inserted_count", 0)))
            with c2:
                render_metric_card("跳过重复", str(ingest_stats.get("skipped_count", 0)))
            with c3:
                render_metric_card("失败", str(ingest_stats.get("failed_count", 0)))
            with c4:
                render_metric_card("补抓模式", safe_text(fetch_plan.get("mode"), "未触发"))

            st.markdown("")
            st.markdown(f"**是否触发补抓：** {'是' if need_fetch else '否'}")

            if need_fetch:
                st.markdown(f"**缺失年份：** {safe_text(fetch_plan.get('insufficient_years'))}")
                st.markdown(f"**每年抓取上限：** {fetch_plan.get('year_fetch_count', 0)}")

                success_years = fetch_plan.get("success_years", [])
                failed_years = fetch_plan.get("failed_years", [])
                empty_years = fetch_plan.get("empty_years", [])

                st.markdown(f"**成功年份/任务：** {success_years if success_years else '无'}")
                st.markdown(f"**失败年份/任务：** {failed_years if failed_years else '无'}")
                st.markdown(f"**空结果年份/任务：** {empty_years if empty_years else '无'}")
            else:
                st.info("本次数据库数据已满足基础分析要求，未触发补抓。")

        # 正式报告
        with subtab3:
            st.markdown("#### 报告内容")

            report_tab1, report_tab2 = st.tabs(["简版报告", "正式报告"])

            with report_tab1:
                simple_report_html = safe_text(report_text, "暂无简版报告内容。").replace("\n", "<br>")
                st.markdown(
                    f"""
                    <div class="report-box">
                        {simple_report_html}
                    </div>
                    """,
                    unsafe_allow_html=True
                )

            with report_tab2:
                final_report_html = safe_text(final_report_text, "暂无正式报告内容。").replace("\n", "<br>")
                st.markdown(
                    f"""
                    <div class="report-box">
                        {final_report_html}
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        # 热点分析
        with subtab4:
            col_hot_1, col_hot_2 = st.columns(2)

            with col_hot_1:
                render_hotspot_list("整体技术热点", hotspots)

            with col_hot_2:
                render_hotspot_list("最新技术热点", latest_hotspots)

        # 时间线
        with subtab5:
            st.markdown("#### 技术演进时间线")
            render_timeline(timeline_data)

        # 论文样本
        with subtab6:
            paper_tab1, paper_tab2 = st.tabs(["代表性论文", "最新论文"])

            with paper_tab1:
                if not papers:
                    st.info("暂无代表性论文。")
                else:
                    for i, paper in enumerate(papers, 1):
                        render_paper_card(i, paper)

            with paper_tab2:
                if not latest_papers:
                    st.info("暂无最新论文。")
                else:
                    for i, paper in enumerate(latest_papers, 1):
                        render_paper_card(i, paper)

# =========================
# Tab 2: 数据库管理（第一阶段骨架）
# =========================
with tab_database:
    st.markdown("### 数据库管理")

    query_service = PaperQueryService()

    MAIN_TOPICS = [
        "人工智能与计算机",
        "医学与生命科学",
        "材料与化学",
        "物理与天文",
        "工程与控制",
        "教育与社会科学",
        "环境与地球科学",
    ]

    selected_main_topic = st.selectbox(
        "选择主分类",
        options=MAIN_TOPICS,
        index=0
    )

    db_limit = st.selectbox(
        "展示论文数量",
        options=[10, 20, 30, 50],
        index=1
    )

    if st.button("查看该分类论文", use_container_width=True):
        try:
            with st.spinner("正在读取数据库分类论文..."):
                papers = query_service.get_papers_by_main_topic(
                    main_topic=selected_main_topic,
                    limit=db_limit
                )

                topic_status = query_service.is_topic_data_enough(
                    main_topic=selected_main_topic,
                    min_paper_count=15,
                    years=5,
                    per_year_min_paper_count=2
                )

                st.session_state.db_query_result = {
                    "main_topic": selected_main_topic,
                    "papers": papers,
                    "topic_status": topic_status,
                    "year_counts": topic_status.get("year_counts", {}),
                    "insufficient_years": topic_status.get("insufficient_years", []),
                }
                st.session_state.db_query_error = None

        except Exception as e:
            st.session_state.db_query_result = None
            st.session_state.db_query_error = str(e)

    if st.session_state.db_query_error:
        st.error(f"数据库读取失败：{st.session_state.db_query_error}")

    db_result = st.session_state.db_query_result

    if db_result is None:
        st.info("请选择一个主分类，然后查看数据库中的论文。")
    else:
        main_topic = db_result.get("main_topic", "")
        papers = db_result.get("papers", [])
        topic_status = db_result.get("topic_status", {})
        year_counts = db_result.get("year_counts", {})
        insufficient_years = db_result.get("insufficient_years", [])

        st.markdown("---")
        st.markdown("### 分类概览")

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            render_metric_card("当前主分类", safe_text(main_topic))
        with c2:
            render_metric_card("数据库论文数", str(topic_status.get("paper_count", len(papers))))
        with c3:
            render_metric_card("最新论文日期", safe_text(topic_status.get("latest_published_date"), "暂无"))
        with c4:
            render_metric_card("是否达到分析阈值", "是" if topic_status.get("enough", False) else "否")

        subtab_db1, subtab_db2, subtab_db3 = st.tabs(
            ["论文列表", "年度分布", "缺失年份"]
        )

        with subtab_db1:
            st.markdown("#### 分类论文展示")
            if not papers:
                st.warning("该分类下暂无论文。")
            else:
                for i, paper in enumerate(papers, 1):
                    render_paper_card(i, paper)

        with subtab_db2:
            st.markdown("#### 近五年年度分布")
            render_year_distribution(year_counts)

        with subtab_db3:
            st.markdown("#### 缺失年份")
            if insufficient_years:
                st.warning("以下年份论文仍不足：" + "、".join([str(y) for y in insufficient_years]))
            else:
                st.success("当前近五年论文分布暂无明显缺口。")

            st.markdown("")
            st.info("下一步这里会继续接“按缺失年份补充论文”的功能。")