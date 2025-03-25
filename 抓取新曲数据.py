import asyncio
from bilibili_api import video_zone
from pathlib import Path
import os
import json
from src.bilibili_scraper import Config, SearchOptions, BilibiliScraper

async def main():
    config = Config(OUTPUT_DIR=Path("新曲数据"))
    if os.path.exists('keywords.json'):
        with open('keywords.json', encoding='utf-8') as file:
            config.KEYWORDS = json.load(file)

    scraper = BilibiliScraper(mode="new", days=2, config=config)
    scraper.search_options = SearchOptions(video_zone_type=video_zone.VideoZoneTypes.MUSIC)
    videos = await scraper.process_new_songs()
    await scraper.save_to_excel(videos)
    await scraper.close_session()

if __name__ == "__main__":
    asyncio.run(main())