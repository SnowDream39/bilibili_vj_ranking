import asyncio
from bilibili_api import search
from pathlib import Path
from src.bilibili_scraper import Config, SearchOptions, BilibiliScraper

config = Config(
    KEYWORDS=['夢ノ結唱', '梦的结唱', 'Syntheiszer ROSE', 'Synthesizer POPY', 'ksm', 'ykn', '香澄', '友希那'],
    OUTPUT_DIR=Path('特殊/特殊原始数据'),
    NAME="梦的结唱4"
)

search_options = SearchOptions(
    order_type = search.OrderVideo.CLICK,
    time_start= '2025-02-01',
    time_end = '2025-03-27',
)

async def main():
    scraper = BilibiliScraper(mode='special', config=config, search_options=search_options)
    videos = await scraper.process_new_songs()
    await scraper.save_to_excel(videos)
    await scraper.close_session()

if __name__ == "__main__":
    asyncio.run(main())