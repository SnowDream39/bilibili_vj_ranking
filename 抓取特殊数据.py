# 抓取特殊数据.py
import asyncio
from bilibili_api import search
from pathlib import Path
from src.bilibili_scraper import Config, SearchOptions, BilibiliScraper, SearchRestrictions
import yaml
import json

with open('config/特殊.yaml', 'r', encoding='utf-8') as file:
    config_file = yaml.safe_load(file)


with open('config/keywords.json', 'r', encoding='utf-8') as file:
    keywords = json.load(file)


config = Config(
    KEYWORDS= keywords,
    OUTPUT_DIR=Path('特殊/特殊原始数据'),
    NAME= config_file['name']
)

restrictions = SearchRestrictions(
    min_favorite= 10000,
)

search_options = [SearchOptions(
    order_type = search.OrderVideo.STOW,
    video_zone_type = i
) for i in [0]]

async def main():
    scraper = BilibiliScraper(
        mode='special', 
        config=config, 
        search_options=search_options, 
        search_restrictions=restrictions,
    )
    videos = await scraper.process_new_songs()
    await scraper.save_to_excel(videos)
    await scraper.close_session()

if __name__ == "__main__":
    asyncio.run(main())
    