import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
from typing import List, Dict
import re


class CNNewsFetcher:
    """
    中文新闻获取模块
    支持根据用户输入关键词，从多个中文新闻搜索源获取新闻
    当前实现：
    1. 百度新闻搜索
    2. 今日头条搜索（补充）
    """

    def __init__(self, max_results: int = 10):
        self.max_results = max_results
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        }

    def fetch(self, keyword: str) -> List[Dict]:
        results = []

        # 先抓百度新闻
        results.extend(self._fetch_baidu_news(keyword))

        # 再抓头条补充
        if len(results) < self.max_results:
            results.extend(self._fetch_toutiao_news(keyword))

        results = self._deduplicate(results)
        return results[:self.max_results]

    def _deduplicate(self, news_list: List[Dict]) -> List[Dict]:
        seen = set()
        result = []

        for item in news_list:
            title = item.get("title", "").strip()
            url = item.get("url", "").strip()

            if not title or not url:
                continue

            key = (title, url)
            if key not in seen:
                seen.add(key)
                result.append(item)

        return result

    def _fetch_baidu_news(self, keyword: str) -> List[Dict]:
        news_list = []
        encoded_keyword = quote(keyword)
        url = f"https://news.baidu.com/ns?word={encoded_keyword}"

        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            response.encoding = response.apparent_encoding

            soup = BeautifulSoup(response.text, "lxml")

            # 百度新闻页面结构可能变化，这里采用较宽松策略：
            # 先找所有标题链接，再结合周围文本补摘要/来源/时间
            for a in soup.find_all("a", href=True):
                title = a.get_text(strip=True)
                link = a["href"]

                if not title or len(title) < 8:
                    continue
                if link.startswith("javascript:") or link == "#":
                    continue

                # 取父节点附近文本作为补充信息
                container_text = a.parent.get_text(" ", strip=True) if a.parent else ""
                summary = ""
                source = "百度新闻"
                published = ""

                # 尝试从附近文本中提取时间
                time_match = re.search(r"(\d{4}[-/年]\d{1,2}[-/月]\d{1,2}日?|\d+小时前|\d+分钟前)", container_text)
                if time_match:
                    published = time_match.group(1)

                # 尝试猜测摘要：去掉标题后剩下的文本
                if container_text and len(container_text) > len(title):
                    summary = container_text.replace(title, "").strip()

                news = {
                    "title": title,
                    "source": source,
                    "published": published,
                    "summary": summary,
                    "url": link
                }
                news_list.append(news)

                if len(news_list) >= self.max_results:
                    break

        except requests.RequestException as e:
            print("百度新闻抓取失败：", e)

        return news_list

    def _fetch_toutiao_news(self, keyword: str) -> List[Dict]:
        news_list = []
        encoded_keyword = quote(keyword)
        url = f"https://so.toutiao.com/search?keyword={encoded_keyword}"

        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            response.encoding = response.apparent_encoding

            soup = BeautifulSoup(response.text, "lxml")

            for a in soup.find_all("a", href=True):
                title = a.get_text(strip=True)
                link = a["href"]

                if not title or len(title) < 8:
                    continue
                if link.startswith("javascript:") or link == "#":
                    continue

                container_text = a.parent.get_text(" ", strip=True) if a.parent else ""
                summary = ""
                source = "今日头条"
                published = ""

                time_match = re.search(r"(\d{4}[-/年]\d{1,2}[-/月]\d{1,2}日?|\d+小时前|\d+分钟前)", container_text)
                if time_match:
                    published = time_match.group(1)

                if container_text and len(container_text) > len(title):
                    summary = container_text.replace(title, "").strip()

                news = {
                    "title": title,
                    "source": source,
                    "published": published,
                    "summary": summary,
                    "url": link
                }
                news_list.append(news)

                if len(news_list) >= self.max_results:
                    break

        except requests.RequestException as e:
            print("今日头条抓取失败：", e)

        return news_list