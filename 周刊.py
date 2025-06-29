# 周刊.py
import asyncio
from src.ranking_processor import RankingProcessor
from utils.config_handler import ConfigHandler

async def main():
    dates = ConfigHandler.get_weekly_dates()
    processor = RankingProcessor(period='weekly')
    await processor.run(dates=dates)

if __name__ == "__main__":
    asyncio.run(main())
