# 抓取新曲数据.py
import asyncio
from pathlib import Path
import json
from src.bilibili_scraper import Config, SearchOptions, BilibiliScraper

with open('config/keywords.json', 'r', encoding='utf-8') as file:
    keywords = json.load(file)

config = Config(
    KEYWORDS= keywords,
    OUTPUT_DIR=Path('测试内容'),
)

async def main():
    scraper = BilibiliScraper(mode="new", days=2, config=config)
    scraper.search_options = [SearchOptions(video_zone_type=i) for i in [3,47]]
    videos = await scraper.process_new_songs()
    await scraper.save_to_excel(videos)
    await scraper.close_session()

if __name__ == "__main__":
    asyncio.run(main())