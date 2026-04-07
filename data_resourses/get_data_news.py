import feedparser
import urllib.parse


class RSSNewsFetcher:

    BASE_URL = "https://www.bing.com/news/search?q={}&format=rss"

    def __init__(self, max_results=5):
        self.max_results = max_results

    def fetch(self, keyword):

        keyword = urllib.parse.quote(keyword)

        url = self.BASE_URL.format(keyword)

        feed = feedparser.parse(url)

        news_list = []

        for entry in feed.entries[:self.max_results]:

            news = {
                "title": entry.get("title"),
                "source": entry.get("source", {}).get("title", "Unknown"),
                "published": entry.get("published", ""),
                "summary": entry.get("summary", ""),
                "url": entry.get("link")
            }

            news_list.append(news)

        return news_list