# 月刊.py
import asyncio
from src.ranking_processor import RankingProcessor
from utils.config_handler import ConfigHandler

async def main():
    dates = ConfigHandler.get_monthly_dates()
    processor = RankingProcessor(period='monthly')
    await processor.run(dates=dates)

if __name__ == "__main__":
    asyncio.run(main())
