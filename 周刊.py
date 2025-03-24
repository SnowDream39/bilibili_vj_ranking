import os
import pandas as pd
from datetime import datetime, timedelta
from src.processing import process_records
from utils.calculator import calculate_ranks, update_rank_and_rate, update_count
from utils.io_utils import format_columns, save_to_excel

CONFIG = {
    "ranking_type": "weekly",
    "columns": [
        'title', 'bvid', 'name', 'author', 'uploader', 'copyright', 
        'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 'page', 
        'view', 'favorite', 'coin', 'like', 'image_url'
    ],
    "dates": {
        "old": '20250315',
        "new": '20250322',
        "target": '2025-03-22',
    },
    "output_paths": {
        "total": "周刊/总榜",
        "new_song": "周刊/新曲榜"
    }
}
CONFIG["dates"]["previous"] = f"{CONFIG['dates']['old'][:4]}-{CONFIG['dates']['old'][4:6]}-{CONFIG['dates']['old'][6:]}"

def merge_old_data(date, columns):
    main_data = pd.read_excel(f"数据/{date}.xlsx", usecols=columns)
    new_song_file = f"新曲数据/新曲{date}.xlsx"
    if os.path.exists(new_song_file):
        new_song_data = pd.read_excel(new_song_file, usecols=columns)
        merged_data = pd.concat([main_data, new_song_data]).drop_duplicates(subset=['bvid'], keep='first')
        return merged_data
    else:
        return main_data
    
def filter_new_songs(df, top_20_names):
    """筛选新曲"""
    start_date = datetime.strptime(CONFIG['dates']['old'], "%Y%m%d") - timedelta(days=7)
    end_date = datetime.strptime(CONFIG['dates']['new'], "%Y%m%d")
    
    df['pubdate'] = pd.to_datetime(df['pubdate'])
    mask = ((df['pubdate'] >= start_date) & (df['pubdate'] < end_date) & (~df['name'].isin(top_20_names)))
    return df[mask].copy()

def main():
    old_data = merge_old_data(CONFIG['dates']['old'], CONFIG['columns'])
    new_data = pd.read_excel(f"数据/{CONFIG['dates']['new']}.xlsx", usecols=CONFIG['columns'])

    df = process_records(
        new_data=new_data,
        old_data=old_data,
        use_old_data=True,
        old_time_toll=CONFIG['dates']['old'],
        ranking_type=CONFIG['ranking_type']
    )
    df = df.loc[df.groupby('name')['point'].idxmax()].reset_index(drop=True)
    df = calculate_ranks(df)
    df = format_columns(df)
    df = update_count(df, f"{CONFIG['output_paths']['total']}/{CONFIG['dates']['previous']}.xlsx")
    df = update_rank_and_rate(df, f"{CONFIG['output_paths']['total']}/{CONFIG['dates']['previous']}.xlsx")
    
    save_to_excel(df, f"{CONFIG['output_paths']['total']}/{CONFIG['dates']['target']}.xlsx")

    top_20_names = set(df[df['count'] > 0]['name'])
    new_songs_df = filter_new_songs(df, top_20_names)
    
    if not new_songs_df.empty:
        new_songs_df = calculate_ranks(new_songs_df)
        new_songs_df = format_columns(new_songs_df)
        save_to_excel(new_songs_df, f"{CONFIG['output_paths']['new_song']}/新曲{CONFIG['dates']['target']}.xlsx")

if __name__ == "__main__":
    main()