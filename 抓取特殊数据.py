# 抓取特殊数据.py
import asyncio
from bilibili_api import search
from pathlib import Path
import yaml
import json

from src.bilibili_scraper import Config, SearchOptions, BilibiliScraper, SearchRestrictions
from src.bilibili_api_client import BilibiliApiClient


with open('config/特殊.yaml', 'r', encoding='utf-8') as file:
    config_file = yaml.safe_load(file)

config = Config(
    KEYWORDS= config_file['keywords'],
    OUTPUT_DIR=Path('特殊/特殊原始数据'),
    NAME= config_file['name']
)

restrictions = SearchRestrictions(
)

search_options = [SearchOptions(
    order_type = search.OrderVideo.PUBDATE,
    time_start = "2025-09-27",
    time_end = "2025-11-27",
    video_zone_type = 0 )
]

async def main():
    api_client = BilibiliApiClient(config=config)
    scraper = BilibiliScraper(
        api_client=api_client,
        mode='special', 
        config=config, 
        search_options=search_options, 
        search_restrictions=restrictions,
    )
    try:
        videos = await scraper.process_new_songs()
        await scraper.save_to_excel(videos)
    finally:
        await api_client.close_session()

if __name__ == "__main__":
    asyncio.run(main())