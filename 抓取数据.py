import asyncio
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, Set, Union
from dataclasses import dataclass
from bilibili_api import video
from datetime import timedelta

def get_save_date():
    now = datetime.now()
    if now.hour >= 23:
        next_day = now + timedelta(days=1)
        return next_day.strftime('%Y%m%d')
    return now.strftime('%Y%m%d')

@dataclass
class VideoData:
    title: str
    bvid: str
    name: str
    author: str
    uploader: str
    copyright: int
    synthesizer: str
    vocal: str
    type: str
    pubdate: str
    duration: str
    page: int
    view: int
    favorite: int
    coin: int
    like: int
    image_url: str

class SongDataFetcher:
    def __init__(self, input_file: Union[str, Path], output_dir: Union[str, Path]):
        self.input_file = Path(input_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.error_indices: Set[int] = set()
        self.fetched_data = pd.DataFrame()
        self.songs = pd.read_excel(self.input_file)
        self._ensure_datatypes()
        self.semaphore = None 
        
    async def initialize(self):
        self.semaphore = asyncio.Semaphore(5)

    def _ensure_datatypes(self) -> None:
        required_columns = ['name', 'bvid', 'author', 'copyright', 'synthesizer', 'vocal', 'type']
        missing_columns = set(required_columns) - set(self.songs.columns)
        if missing_columns:
            raise ValueError(f"输入文件缺少必要列: {missing_columns}")
        
        self.songs['bvid'] = self.songs['bvid'].astype(str)

    @staticmethod
    def convert_duration(duration: int) -> str:
        duration = max(0, duration - 1)
        minutes, seconds = divmod(duration, 60)
        return f'{minutes}分{seconds}秒' if minutes > 0 else f'{seconds}秒'

    async def fetch_song_stat(self, index: int) -> Optional[VideoData]:
        async with self.semaphore:
            song = self.songs.loc[index]
            print(f"正在获取: {song['name']} ({song['bvid']})")
            
            try:
                v = video.Video(bvid=song['bvid'])
                info = await v.get_info()
                await asyncio.sleep(0.8) 
       
                stat = info['stat']
                owner = info['owner']
                return VideoData(
                    title= info['title'],
                    bvid= song['bvid'],
                    name= song['name'],
                    author = song['author'],
                    uploader= owner['name'],
                    copyright= song['copyright'],
                    synthesizer= song['synthesizer'],
                    vocal = song['vocal'],
                    type = song['type'],
                    pubdate = datetime.fromtimestamp(info['pubdate']).strftime('%Y-%m-%d %H:%M:%S'),
                    duration = self.convert_duration(info['duration']),
                    page = len(info['pages']),
                    view = stat['view'],
                    favorite = stat['favorite'],
                    coin = stat['coin'],
                    like = stat['like'],
                    image_url = info['pic']
                )        
            except Exception as e:
                print(f"获取失败: {song['name']} ({song['bvid']}), 错误: {e}")
                self.error_indices.add(index)
                return None

    async def run_tasks_with_retries(self, indices: Set[int], max_retries: int = 3) -> None:
        current_indices = indices
        
        for attempt in range(max_retries):
            if not current_indices:
                break
                
            tasks = [self.fetch_song_stat(i) for i in current_indices]
            results = [r for r in await asyncio.gather(*tasks) if r is not None]
            new_data = pd.DataFrame([vars(r) for r in results])
            self.fetched_data = pd.concat([self.fetched_data, new_data], ignore_index=True)
            current_indices = self.error_indices
            self.error_indices = set()
            
            if current_indices:
                print(f"第 {attempt + 1} 次重试，剩余 {len(current_indices)} 个任务...")

    async def save_data(self) -> None:
        if self.fetched_data.empty:
            print("没有获取到任何数据")
            return
        timestamp = get_save_date()
        detail_file = self.output_dir / f"{timestamp}.xlsx"
        self.fetched_data.to_excel(detail_file, index=False)
        print(f"{detail_file} 已保存")
        update_columns = ['title', 'bvid', 'name', 'view', 'pubdate', 'author', 'uploader', 
                         'copyright', 'synthesizer', 'vocal', 'type', 'image_url']
        update_data = self.fetched_data[update_columns].sort_values('view', ascending=False)
        
        original_data = pd.read_excel(self.input_file)
        merged_data = pd.concat([original_data, update_data]).drop_duplicates(subset=['bvid'], keep='last')
        merged_data.to_excel(self.input_file, index=False)
        print(f"{self.input_file} 已更新")

    async def run(self) -> None:
        await self.initialize() 
        await self.run_tasks_with_retries(set(self.songs.index))
        await self.save_data()

if __name__ == "__main__":
    async def main():
        fetcher = SongDataFetcher('催眠者.xlsx', '特殊\特殊原始数据')
        await fetcher.run()

    asyncio.run(main())