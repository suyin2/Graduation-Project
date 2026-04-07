import time
from urllib.parse import quote

import feedparser
import requests


BASE_URL = "https://export.arxiv.org/api/query"
REQUEST_TIMEOUT = 30


def build_url(keyword: str, max_results: int = 5) -> str:
    keyword = (keyword or "").strip()
    if not keyword:
        raise ValueError("关键词不能为空")

    search_query = f"all:({keyword})"
    encoded_query = quote(search_query, safe="():[]\"")

    url = (
        f"{BASE_URL}"
        f"?search_query={encoded_query}"
        f"&start=0"
        f"&max_results={max_results}"
        f"&sortBy=submittedDate"
        f"&sortOrder=descending"
    )
    return url


def test_arxiv_once(keyword: str = "machine learning", max_results: int = 5):
    url = build_url(keyword, max_results=max_results)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; arXivTest/1.0)"
    })

    print("准备请求 arXiv API...")
    print(f"关键词: {keyword}")
    print(f"数量: {max_results}")
    print("说明: 本测试只发送 1 次请求，不做重试。")
    print("-" * 80)

    try:
        start_time = time.time()
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        elapsed = time.time() - start_time

        print(f"HTTP 状态码: {response.status_code}")
        print(f"耗时: {elapsed:.2f} 秒")

        response.raise_for_status()

        feed = feedparser.parse(response.text)
        entries = getattr(feed, "entries", [])

        print(f"实际返回论文数: {len(entries)}")
        print("-" * 80)

        if not entries:
            print("请求成功，但没有返回论文。")
            return

        for i, entry in enumerate(entries, start=1):
            title = getattr(entry, "title", "").replace("\n", " ").strip()
            published = getattr(entry, "published", "")
            link = getattr(entry, "link", "")

            print(f"{i}. {title}")
            print(f"   发布时间: {published}")
            print(f"   链接: {link}")
            print()

        print("测试完成：这次请求成功，说明当前至少可以正常访问 arXiv API。")

    except requests.HTTPError as e:
        print(f"HTTP 请求失败: {e}")
        if getattr(e, "response", None) is not None:
            print(f"响应状态码: {e.response.status_code}")
            print(f"响应内容前 300 字符: {e.response.text[:300]}")

    except requests.ReadTimeout:
        print("请求超时：30 秒内没有收到完整响应。")
        print("这不一定只是本地网络问题，也可能是 arXiv 响应慢。")

    except requests.RequestException as e:
        print(f"请求异常: {e}")

    except Exception as e:
        print(f"解析或其他异常: {e}")


if __name__ == "__main__":
    test_arxiv_once(keyword="machine learning", max_results=5)