from services.paper_query_service import PaperQueryService
from llm.hotspot_extractor import HotspotExtractor

query_service = PaperQueryService()
extractor = HotspotExtractor()

papers = query_service.get_representative_papers(
    query_text="machine learning",
    main_topic="人工智能与计算机",
    sub_topic="",
    limit=8,
    fetch_limit=100,
    years=5
)

hotspots = extractor.extract_hotspots(
    keyword="machine learning",
    papers=papers,
    top_k=6
)

print("==== 代表性论文数 ====")
print(len(papers))

print("==== 热点提取结果 ====")
for item in hotspots:
    print(item)