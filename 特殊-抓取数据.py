import asyncio
from bilibili_api import search
from pathlib import Path
from src.bilibili_scraper import Config, SearchOptions, BilibiliScraper
from utils.clash import Clash
import yaml

with open('config/特殊.yaml', 'r', encoding='utf-8') as file:
    config_file = yaml.safe_load(file)


config = Config(
    KEYWORDS= config_file['keywords'],
    OUTPUT_DIR=Path('特殊/特殊原始数据'),
    NAME= config_file['name']
)

search_options = SearchOptions(
    order_type = search.OrderVideo.CLICK,
    time_start= '2025-03-26',
    time_end = '2025-05-27',
)

async def main():
    scraper = BilibiliScraper(mode='special', config=config, search_options=search_options, proxy=Clash())
    videos = await scraper.process_new_songs()
    await scraper.save_to_excel(videos)
    await scraper.close_session()

if __name__ == "__main__":
    asyncio.run(main())