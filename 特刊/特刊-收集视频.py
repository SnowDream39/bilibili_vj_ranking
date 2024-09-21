import asyncio
import aiohttp
import pandas as pd
from bilibili_api import search, video, Credential, video_zone
from datetime import datetime, timedelta
import re
import random
from openpyxl.utils import get_column_letter
from bilibili_api import settings


KEYWORDS = ["夢ノ結唱","梦的结唱"]
VIDEO_ZONE_TYPES = [video_zone.VideoZoneTypes.MUSIC]

class BilibiliScraper:
    def __init__(self, keywords, video_zone_types, days=None, max_retries=3, headers_list=None):
        self.keywords = keywords
        self.video_zone_types = video_zone_types
        self.max_retries = max_retries
        self.headers_list = headers_list or [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.67 Safari/537.36',
        ]
    @staticmethod
    def clean_html_tags(text):
        clean = re.compile('<.*?>')
        return re.sub(clean, '', text)

    @staticmethod
    def convert_duration(duration):
        duration -= 1
        minutes, seconds = divmod(duration, 60)
        return f'{minutes}分{seconds}秒' if minutes > 0 else f'{seconds}秒'
    
    async def search_videos(self, keyword, video_zone_type):
        bvids = []
        page = 1

        while True:
            print(f"Searching for keyword {keyword} in zone {video_zone_type}, page: {page}")
            result = await search.search_by_type(
                keyword,
                search_type=search.SearchObjectType.VIDEO,
                order_type=search.OrderVideo.CLICK,
                video_zone_type=video_zone_type,
                page=page
            )

            videos = result.get('result', [])
            if not videos:
                break

            for item in videos:
                bvids.append(item['bvid'])
                print(f"Found video: {item['bvid']}")
                
            page += 1
            await asyncio.sleep(0.5)

        return bvids

    async def get_all_bvids(self):
        all_bvids = []

        for keyword in self.keywords:
            print(f"Processing keyword: {keyword}")
            for video_zone_type in self.video_zone_types:
                bvids = await self.search_videos(keyword, video_zone_type)
                all_bvids.extend(bvids)

        return list(set(all_bvids))

    async def fetch_video_details(self, bvid):
        for attempt in range(self.max_retries):
            try:
                print(f"Fetching details for BVID: {bvid}, attempt: {attempt + 1}")
                v = video.Video(bvid, credential=Credential())
                info = await v.get_info()
                duration_seconds = info['duration']
                if duration_seconds <= 20:
                    print(f"Skipping video with duration less than 20 seconds: {bvid}")
                    return None
                pubdate = datetime.fromtimestamp(info['pubdate'])
                return {
                    'video_title': self.clean_html_tags(info['title']),
                    'bvid': bvid,
                    'author': info['owner']['name'],
                    'copyright': info['copyright'],
                    'pubdate': pubdate.strftime('%Y-%m-%d %H:%M:%S'),
                    'duration': self.convert_duration(duration_seconds),
                    'view': info['stat']['view'],
                    'favorite': info['stat']['favorite'],
                    'coin': info['stat']['coin'],
                    'like': info['stat']['like']
                }
            except Exception as e:
                print(f"Error fetching details for BVID: {bvid}, attempt {attempt + 1}/{self.max_retries}, Error: {e}")
                await asyncio.sleep(0.8)

        return None

    async def get_video_details(self, bvids):
        sem = asyncio.Semaphore(5)

        async def sem_fetch(bvid):
            async with sem:
                result = await self.fetch_video_details(bvid)
                await asyncio.sleep(0.8)
                return result

        tasks = [sem_fetch(bvid) for bvid in bvids]
        results = await asyncio.gather(*tasks)
        return [result for result in results if result]

    async def get_all_videos(self):
        print("Starting to get all BVIDs")
        bvids = await self.get_all_bvids()
        print(f"Total BVIDs found: {len(bvids)}")
        videos = await self.get_video_details(bvids)

        for video in videos:
            video['title'] = video['video_title']
            video['uploader'] = video['author']
            video['synthesizer'] = ""
            video['vocal'] = ""

        return videos

    async def save_to_excel(self, videos):
        df = pd.DataFrame(videos)
        df = df.sort_values(by='view', ascending=False)
        df = df[['video_title', 'bvid', 'title', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 'view', 'favorite', 'coin', 'like']]
        filename = f"特殊/特殊原始数据/梦的结唱-初步收录范围.xlsx"

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            worksheet = writer.sheets['Sheet1']

            col_letter = get_column_letter(df.columns.get_loc('pubdate') + 1)
            for cell in worksheet[col_letter]:
                cell.number_format = '@'
                cell.alignment = cell.alignment.copy(horizontal='left')

        print("处理完成，数据已保存到", filename)

async def main():
    scraper = BilibiliScraper(KEYWORDS, VIDEO_ZONE_TYPES)
    videos = await scraper.get_all_videos()
    await scraper.save_to_excel(videos)

if __name__ == "__main__":
    asyncio.run(main())
