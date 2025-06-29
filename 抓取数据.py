# 抓取数据.py
import asyncio
import json
from pathlib import Path
from src.bilibili_scraper import Config, BilibiliScraper

async def main():
    config = Config(OUTPUT_DIR=Path("数据"))
    scraper = BilibiliScraper(mode="old", config=config, input_file="收录曲目.xlsx")
    videos = await scraper.process_old_songs()
    await scraper.save_to_excel(videos, usecols=json.load(Path('config/usecols.json').open(encoding='utf-8'))["columns"]['stat'])
    await scraper.close_session()
    
if __name__ == "__main__":
    asyncio.run(main())