# 抓取数据.py
import asyncio
import json
from pathlib import Path
from src.bilibili_scraper import Config, BilibiliScraper
from src.bilibili_api_client import BilibiliApiClient

async def main():
    config = Config(OUTPUT_DIR=Path("测试内容"))
    api_client = BilibiliApiClient(config=config)
    scraper = BilibiliScraper(api_client=api_client, mode="old", config=config, input_file="收录曲目.xlsx")
    try:
        videos = await scraper.process_old_songs()
        usecols = json.load(Path('config/usecols.json').open(encoding='utf-8'))["columns"]['stat']
        await scraper.save_to_excel(videos, usecols=usecols)
    finally:
        await api_client.close_session()
    
if __name__ == "__main__":
    asyncio.run(main())
