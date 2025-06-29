# 计算数据.py
import asyncio
from src.ranking_processor import RankingProcessor

async def main():
    processor = RankingProcessor(period='daily')
    await processor.run()

if __name__ == "__main__":
    asyncio.run(main())
