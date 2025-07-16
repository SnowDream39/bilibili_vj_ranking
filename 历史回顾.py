import asyncio
from src.ranking_processor import RankingProcessor
from utils.config_handler import ConfigHandler

async def main():
    """生成历史回顾榜单"""
    dates = ConfigHandler.get_history_dates() 
    processor = RankingProcessor(period='history')
    await processor.run(dates=dates)

if __name__ == "__main__":
    asyncio.run(main())