# 新曲排行榜.py
import asyncio
from src.ranking_processor import RankingProcessor

async def main():
    processor = RankingProcessor(period='daily_new_song')
    await processor.run()

if __name__ == "__main__":
    asyncio.run(main())
