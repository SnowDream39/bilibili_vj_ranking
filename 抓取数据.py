import asyncio
import re
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, Set, Union
from dataclasses import dataclass
from bilibili_api import video
from datetime import timedelta
import os
import json

from utils.bilibili_scraper import VideoInfo, Config, SearchOptions, BilibiliScraper
from utils.clash import Clash



async def main():
    config = Config(OUTPUT_DIR=Path("新曲数据"))
    if os.path.exists('keywords.json'):
        with open('keywords.json', encoding='utf-8') as file:
            config.KEYWORDS = json.load(file)
        config.OUTPUT_DIR = Path('数据')

    scraper = BilibiliScraper(mode="main", input_file="收录曲目.xlsx", config=config, proxy=Clash())
    videos = await scraper.process_old_songs()
    scraper.save_to_excel(videos)

if __name__ == "__main__":


    asyncio.run(main())