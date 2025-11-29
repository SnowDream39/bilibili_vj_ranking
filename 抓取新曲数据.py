# 抓取新曲数据.py
import asyncio
from pathlib import Path
import json
from src.bilibili_scraper import Config, SearchOptions, BilibiliScraper
from src.bilibili_api_client import BilibiliApiClient

with open('config/keywords.json', 'r', encoding='utf-8') as file:
    keywords = json.load(file)

async def main():
    config = Config(KEYWORDS=keywords, OUTPUT_DIR=Path('测试内容'))
    search_options = [
        SearchOptions(video_zone_type=3),
        SearchOptions(video_zone_type=47),
        SearchOptions(newlist_rids=[30])
    ]
    api_client = BilibiliApiClient(config=config)
    scraper = BilibiliScraper(api_client=api_client, mode="new", days=2, config=config, search_options=search_options)
    try:
        videos = await scraper.process_new_songs()
        await scraper.save_to_excel(videos)
    finally:
        await api_client.close_session()

if __name__ == "__main__":
    asyncio.run(main())
