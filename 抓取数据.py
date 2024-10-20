import asyncio
import subprocess
import pandas as pd
from datetime import datetime, timedelta
from bilibili_api import video

class SongDataFetcher:
    def __init__(self, input_file, output_dir):
        self.input_file = input_file
        self.output_dir = output_dir
        self.songs = pd.read_excel(input_file)
        self.info_list = []
        self.error_list = []
        self.data_list = []
        self.semaphore = asyncio.Semaphore(5) 

    def run_script_in_new_window(self, script):
        subprocess.Popen(['start', 'cmd', '/k', f'python {script}'], shell=True)

    def convert_duration(self, duration):
        duration -= 1
        minutes, seconds = divmod(duration, 60)
        if minutes > 0:
            return f'{minutes}分{seconds}秒'
        else:
            return f'{seconds}秒'

    async def fetch_song_stat(self, i):
        async with self.semaphore:  
            song = self.songs.loc[i]
            print(f"Fetching data for: {song['Title']} ({song['BVID']})")
            v = video.Video(bvid=song['BVID'])
            try:
                info = await v.get_info()
                await asyncio.sleep(0.1)  
            except Exception as e:
                print(f"Error fetching data for: {song['Title']} ({song['BVID']}), error: {e}")
                self.error_list.append(i)
                return

            stat_data = info.get('stat')
            owner_data = info.get('owner')

            video_title = info.get('title')
            uploader = owner_data.get('name')
            duration = self.convert_duration(info.get('duration'))
            page = len(info.get('pages'))
            image_url = info.get('pic')  
            if stat_data and owner_data:
                view = stat_data.get('view')
                favorite = stat_data.get('favorite')
                coin = stat_data.get('coin')
                like = stat_data.get('like')
                
                self.info_list.append([
                    video_title, song['BVID'], song['Title'], 
                    song['Author'], uploader, song['Copyright'],
                    song['Synthesizer'], song['Vocal'], song['Type'], song['Pubdate'], 
                    duration, page, view, favorite, coin, like, image_url
                ])
                self.data_list.append([
                    song['Title'], song['BVID'], video_title, 
                    view, song['Pubdate'], song['Author'], uploader, 
                    song['Copyright'], song['Synthesizer'], song['Vocal'], song['Type'], image_url
                ])
            else:
                print(f"Missing data for: {song['Title']} ({song['BVID']})")
                self.error_list.append(i)
            await asyncio.sleep(0.8) 

    async def fetch_all_stats(self):
        await self.run_tasks_with_retries(self.songs.index)

    async def run_tasks_with_retries(self, indices, max_retries=10):
        for attempt in range(max_retries):
            tasks = [self.fetch_song_stat(i) for i in indices]
            await asyncio.gather(*tasks)

            if not self.error_list:
                break
            print(f"Retrying for error list, attempt {attempt + 1}...")
            indices, self.error_list = self.error_list, []

    def save_data(self):
        if self.info_list:
            stock_list = pd.DataFrame(self.info_list, columns=[
                'video_title', 'bvid', 'title', 'author', 'uploader', 
                'copyright', 'synthesizer', 'vocal', 'type', 'pubdate', 
                'duration', 'page', 'view', 'favorite', 'coin', 'like', 'image_url'
            ])
            filename = f"{self.output_dir}/{(datetime.now() + timedelta(days=1) if datetime.now().hour >= 23 else datetime.now()).strftime('%Y%m%d')}.xlsx"

            stock_list.to_excel(filename, index=False)
            print(f"处理完成，数据已保存到 {filename}")

        if self.data_list:
           
            new_stock_list = pd.DataFrame(self.data_list, columns=[
                'Title', 'BVID', 'Video Title', 'View', 'Pubdate', 
                'Author', 'Uploader', 'Copyright', 'Synthesizer', 'Vocal', 'Type', 'image_url'
            ])
            new_stock_list = new_stock_list.sort_values(by='View', ascending=False)

            original_songs = pd.read_excel(self.input_file)

            merged_stock_list = pd.concat([original_songs, new_stock_list]).drop_duplicates(subset=['BVID'], keep='last')

            merged_stock_list.to_excel(self.input_file, index=False)
            print("收录曲目已更新并按观看数排序")
        else:
            print("没有可用的视频信息，未保存数据")

    async def run(self):
        await self.fetch_all_stats()
        self.save_data()

if __name__ == "__main__":
    fetcher = SongDataFetcher(input_file='收录曲目.xlsx', output_dir='数据')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetcher.run())
