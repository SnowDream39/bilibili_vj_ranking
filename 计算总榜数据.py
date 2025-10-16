# 计算总榜数据.py
import asyncio
from src.ranking_processor import RankingProcessor

async def main():
    processor = RankingProcessor(period='special')
    await processor.run(song_data='弹幕评论')

if __name__ == "__main__":
    asyncio.run(main())
    