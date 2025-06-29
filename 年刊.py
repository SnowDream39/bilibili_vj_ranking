# 年刊.py
import asyncio
from src.ranking_processor import RankingProcessor

async def main():
    """主处理流程：生成年刊"""
    dates = {
        "old_date": "20250101",
        "new_date": "20250629",
        "target_date": "2025",  
    }
    processor = RankingProcessor(period='annual')
    await processor.run(dates=dates)
if __name__ == "__main__":
    asyncio.run(main())