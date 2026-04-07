from database.db import init_db
from database.paper_repository import PaperRepository

init_db()

repo = PaperRepository()

paper = {
    "title": "Test Paper",
    "authors": "Alice, Bob",
    "summary": "This is a test summary.",
    "published": "2026-03-24",
    "link": "https://arxiv.org/abs/1706.03762"
}

repo.insert_paper(
    paper,
    main_topic="大模型",
    topic_en="large language models"
)

row = repo.get_by_paper_id("1706.03762")
print(row)