import asyncio
from pathlib import Path
from src.bilibili_api_client import BilibiliApiClient
from src.bilibili_scraper import BilibiliScraper
from utils.dataclass import Config

async def main():
    """
    执行热门榜视频抓取任务。
    """
    config = Config(OUTPUT_DIR=Path(__file__).parent)
    api_client = BilibiliApiClient(config)
    scraper = BilibiliScraper(
        api_client=api_client,
        mode="hot_rank",
        days=15, 
        config=config
    )
    try:
        await scraper.process_hot_rank_videos()
    finally:
        await scraper.api_client.close_session()

if __name__ == "__main__":
    asyncio.run(main())
