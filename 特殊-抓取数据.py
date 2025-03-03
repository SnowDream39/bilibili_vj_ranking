import asyncio
import aiohttp
import pandas as pd
from bilibili_api import search, video, video_zone, Credential, request_settings
from datetime import datetime, timedelta
import re
import random
from openpyxl.utils import get_column_letter
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any
from pathlib import Path
from utils.clash import Clash

special_name="梦的结唱4"
@dataclass
class VideoInfo:
    """视频信息数据类"""
    title: str
    bvid: str
    name: str
    author: str 
    uploader: str = ""
    copyright: int = 0
    synthesizer: str = ""
    vocal: str = ""
    type: str = ""
    pubdate: str = ""
    duration: str = ""
    tags: str = ""
    description: str = ""
    page: int = 0
    view: int = 0
    favorite: int = 0
    coin: int = 0
    like: int = 0
    image_url: str = ""

class Config:
    """配置类"""
    KEYWORDS = [
        "梦的结唱", "夢ノ結唱"
    ]
    HEADERS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.67 Safari/537.36',
    ]

    MAX_RETRIES = 3
    SEMAPHORE_LIMIT = 5
    MIN_VIDEO_DURATION = 20
    SLEEP_TIME = 0
    OUTPUT_DIR = Path("特殊/特殊原始数据")
    USE_PROXY = True

class RetryHandler:
    """重试处理器"""
    @staticmethod
    async def retry_async(func, *args, max_retries=Config.MAX_RETRIES, **kwargs):
        for attempt in range(max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                print(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(Config.SLEEP_TIME)
        return None

class BilibiliScraper:
    def __init__(self):
        Config.OUTPUT_DIR.mkdir(exist_ok=True)
        if Config.USE_PROXY:
            request_settings.set_proxy('http://127.0.0.1:7897')
            self.clash = Clash()

    @staticmethod
    def clean_html_tags(text: str) -> str:
        return re.sub(r'<.*?>', '', text)

    @staticmethod
    def convert_duration(duration: int) -> str:
        duration -= 1
        minutes, seconds = divmod(duration, 60)
        return f'{minutes}分{seconds}秒' if minutes > 0 else f'{seconds}秒'
    
    @staticmethod
    def remove_illegal_chars(text):
        # 移除非法的控制字符，保留 \t, \n, \r
        return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', text)
    
    async def fetch_data(self, url: str) -> Optional[Dict]:
        async with aiohttp.ClientSession() as session:
            headers = {'User-Agent': random.choice(Config.HEADERS)}
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
        return None

    async def search_videos(self, keyword: str) -> List[str]:
        """搜索指定关键词的所有视频"""
        bvids = []
        page = 1
        max_pages = 50 

        while page <= max_pages:
            try:
                print(f"正在获取关键词 {keyword} 的第 {page} 页")
                result = await search.search_by_type(
                    keyword,
                    search_type=search.SearchObjectType.VIDEO,
                    time_start='2025-02-01',
                    time_end='2025-03-03',
                    video_zone_type=video_zone.VideoZoneTypes.MUSIC,
                    order_type=search.OrderVideo.CLICK,
                    page=page
                )

                videos = result.get('result', [])
                if not videos:
                    print(f"关键词 {keyword} 搜索完成，共 {page-1} 页")
                    break
                for item in videos:
                    bvids.append(item['bvid'])

                page += 1
                await asyncio.sleep(0.2)  

            except Exception as e:
                print(f"获取第 {page} 页时出错: {e}")
                await asyncio.sleep(1)  
                continue
        return bvids

    async def get_all_bvids(self) -> List[str]:
        all_bvids = set()
    
        for keyword in Config.KEYWORDS:
            try:
                if Config.USE_PROXY:
                    self.clash.random_proxy()
                print(f"正在搜索关键词: {keyword}")
                bvids = await self.search_videos(keyword)
                all_bvids.update(bvids)
                print(f"关键词 {keyword} 找到 {len(bvids)} 个视频")
            except Exception as e:
                print(f"搜索关键词 {keyword} 时出错: {e}")
                continue
    
        return list(all_bvids)

    async def fetch_video_details(self, bvid: str) -> Optional[VideoInfo]:
        try:
            if Config.USE_PROXY:
                self.clash.random_proxy()
            v = video.Video(bvid, credential=Credential())
            info = await v.get_info()
            tags = await v.get_tags()
            tags = [tag["tag_name"] for tag in tags]
            if info['duration'] <= Config.MIN_VIDEO_DURATION:
                print(f"跳过短视频： {bvid}")
                return None
            print(f"获取视频信息： {bvid}")
            return VideoInfo(
                title=self.clean_html_tags(info['title']),
                bvid=bvid,
                name=self.clean_html_tags(info['title']),
                author=info['owner']['name'],
                uploader=info['owner']['name'],
                copyright=info['copyright'],
                pubdate=datetime.fromtimestamp(info['pubdate']).strftime('%Y-%m-%d %H:%M:%S'),
                duration=self.convert_duration(info['duration']),
                tags='、'.join(tags),
                description=self.remove_illegal_chars(info['desc']),
                page=len(info['pages']),
                view=info['stat']['view'],
                favorite=info['stat']['favorite'],
                coin=info['stat']['coin'],
                like=info['stat']['like'],
                image_url=info['pic']
            )
        except Exception as e:
            print(f"Error fetching details for {bvid}: {str(e)}")
            return None

    async def get_video_details(self, bvids: List[str]) -> List[VideoInfo]:
        sem = asyncio.Semaphore(Config.SEMAPHORE_LIMIT)
        
        async def sem_fetch(bvid: str) -> Optional[VideoInfo]:
            async with sem:
                result = await RetryHandler.retry_async(self.fetch_video_details, bvid)
                await asyncio.sleep(Config.SLEEP_TIME)
                return result

        tasks = [sem_fetch(bvid) for bvid in bvids]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    async def process_videos(self) -> List[Dict[str, Any]]:
        print("Starting to get all bvids")
        bvids = await self.get_all_bvids()
        print(f"Total bvids found: {len(bvids)}")
        
        videos = await self.get_video_details(bvids)
        return [asdict(video) for video in videos]

    async def save_to_excel(self, videos: List[Dict[str, Any]]) -> None:
        df = pd.DataFrame(videos)
        df = df.sort_values(by='view', ascending=False)

        columns = ['title', 'bvid', 'name', 'author', 'uploader', 'copyright', 
                  'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 'tags','description','page', 
                  'view', 'favorite', 'coin', 'like', 'image_url']
        
        df = df[columns]
        filename = Config.OUTPUT_DIR / f"{special_name}.xlsx"

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            worksheet = writer.sheets['Sheet1']
            
            pubdate_col = get_column_letter(df.columns.get_loc('pubdate') + 1)
            for cell in worksheet[pubdate_col]:
                cell.number_format = '@'
                cell.alignment = cell.alignment.copy(horizontal='left')

        print(f"{filename} 已保存")

async def main():
    scraper = BilibiliScraper()
    videos = await scraper.process_videos()
    await scraper.save_to_excel(videos)

if __name__ == "__main__":
    asyncio.run(main())
