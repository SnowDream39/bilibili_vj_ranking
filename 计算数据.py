import asyncio
import pandas as pd
from datetime import datetime, timedelta
from src.processing import process_records
from utils.io_utils import save_to_excel

today = (datetime.now()-timedelta(days=1)).replace(hour=0, minute=0,second=0,microsecond=0).strftime('%Y%m%d')
    
old_time_toll = datetime.strptime(str(today), '%Y%m%d').strftime('%Y%m%d')
new_time_toll = (datetime.strptime(str(today), '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')  
old_time_new = f'新曲{old_time_toll}'
new_time_new = f'新曲{new_time_toll}'

CONFIG = {
    "columns": [
        'title', 'bvid', 'name', 'author', 'uploader', 'copyright', 'synthesizer',
        'vocal', 'type', 'pubdate', 'duration', 'page', 'view', 'favorite', 'coin',
        'like', 'image_url'
    ],
    "threshold": 1000, # 新曲日增阈值
    "output_paths": {
        "data": "差异/非新曲",
        "new_song": "差异/新曲"
    }
}

def main_processing(old_data_path, new_data_path, output_path, point_threshold=None, data_type=None):
    old_data = pd.read_excel(old_data_path, usecols=CONFIG['columns'])
    new_data = pd.read_excel(new_data_path, usecols=CONFIG['columns'])

    df = process_records(
        new_data=new_data,
        old_data=old_data,
        use_old_data=True,
        use_collected=(data_type == "new_song"),
        collected_data=pd.read_excel('收录曲目.xlsx') if data_type == "new_song" else None,
        ranking_type='daily',
        old_time_toll=old_time_toll
    )
    if point_threshold:
        df = df[df['point'] >= point_threshold]
    df = df.sort_values('point', ascending=False)
    save_to_excel(df, output_path)


async def main():
    await asyncio.gather(
        asyncio.to_thread(main_processing, f'数据/{old_time_toll}.xlsx', f'数据/{new_time_toll}.xlsx', f"{CONFIG['output_paths']['data']}/{new_time_toll}与{old_time_toll}.xlsx"),
        asyncio.to_thread(main_processing, f'新曲数据/{old_time_new}.xlsx', f'新曲数据/{new_time_new}.xlsx', f"{CONFIG['output_paths']['new_song']}/{new_time_new}与{old_time_new}.xlsx", point_threshold=CONFIG['threshold'], data_type="new_song")
    )

if __name__ == "__main__":
    asyncio.run(main())
