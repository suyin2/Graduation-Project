import sqlite3
from pathlib import Path

# 项目根目录下的数据库文件
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "research_assistant.db"


def get_connection():
    """
    获取 SQLite 数据库连接。
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # 开启外键支持
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db():
    """
    初始化数据库，创建所需数据表和索引。
    """
    with get_connection() as conn:
        cursor = conn.cursor()

        # 1. papers 主表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS papers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            paper_id TEXT NOT NULL UNIQUE,
            source TEXT DEFAULT 'arxiv',
            title TEXT NOT NULL,
            authors TEXT,
            abstract TEXT,
            published_date TEXT,
            updated_date TEXT,
            link TEXT UNIQUE,
            pdf_url TEXT,
            pdf_local_path TEXT,

            main_topic TEXT,
            sub_topic TEXT,

            contribution_summary TEXT,
            method_summary TEXT,
            application_scenario TEXT,
            timeline_stage TEXT,

            importance_score REAL DEFAULT 0,
            embedding_status INTEGER DEFAULT 0,
            full_text_status INTEGER DEFAULT 0,

            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at_local TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # 2. paper_categories 分类表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS paper_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            paper_id TEXT NOT NULL,
            category_type TEXT NOT NULL,
            category_value TEXT NOT NULL,
            confidence REAL DEFAULT 0,
            source_method TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (paper_id) REFERENCES papers(paper_id) ON DELETE CASCADE
        );
        """)

        # 3. paper_tags 标签表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS paper_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            paper_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            tag_type TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (paper_id) REFERENCES papers(paper_id) ON DELETE CASCADE
        );
        """)

        # 4. analysis_records 分析记录表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS analysis_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_text TEXT NOT NULL,
            topic TEXT,
            analysis_type TEXT,
            paper_count INTEGER DEFAULT 0,
            time_span_start TEXT,
            time_span_end TEXT,
            result_summary TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # 5. update_logs 更新日志表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS update_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT NOT NULL,
            source TEXT,
            query_used TEXT,
            fetched_count INTEGER DEFAULT 0,
            inserted_count INTEGER DEFAULT 0,
            skipped_count INTEGER DEFAULT 0,
            update_time TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # =========================
        # 创建索引
        # =========================

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_papers_main_topic
        ON papers(main_topic);
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_papers_sub_topic
        ON papers(sub_topic);
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_papers_published_date
        ON papers(published_date);
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_papers_title
        ON papers(title);
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_paper_categories_paper_id
        ON paper_categories(paper_id);
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_paper_categories_type_value
        ON paper_categories(category_type, category_value);
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_paper_tags_paper_id
        ON paper_tags(paper_id);
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_paper_tags_tag
        ON paper_tags(tag);
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_analysis_records_topic
        ON analysis_records(topic);
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_update_logs_topic
        ON update_logs(topic);
        """)

        conn.commit()


if __name__ == "__main__":
    init_db()
    print(f"数据库初始化完成：{DB_PATH}")