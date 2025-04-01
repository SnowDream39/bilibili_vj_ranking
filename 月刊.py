import os
import pandas as pd
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from src.processing import process_records
from utils.calculator import calculate_ranks, update_rank_and_rate
from utils.io_utils import save_to_excel

with open('config/monthly.json','r',encoding='utf-8') as file:
    CONFIG = json.load(file)

CONFIG["dates"]["previous"] = (datetime.strptime(CONFIG['dates']['target'], '%Y-%m') - relativedelta(months=1)).strftime('%Y-%m')
    
def merge_old_data(date, columns):
    main_data = pd.read_excel(f"数据/{date}.xlsx", usecols = columns)
    new_song_file = f"新曲数据/新曲{date}.xlsx"
    if os.path.exists(new_song_file):
        new_song_data = pd.read_excel(new_song_file, usecols = columns)
        merged_data = pd.concat([main_data, new_song_data]).drop_duplicates(subset=['bvid'], keep='first')
        return merged_data
    else:
        return main_data
    
def filter_new_songs(df, top_20_names):
    """筛选新曲"""
    start_date = datetime.strptime(CONFIG['dates']['old'], "%Y%m%d")
    end_date = datetime.strptime(CONFIG['dates']['new'], "%Y%m%d")
    mask = ((pd.to_datetime(df['pubdate']) >= start_date) & (pd.to_datetime(df['pubdate']) < end_date) & (~df['name'].isin(top_20_names)))
    return df[mask].copy()

def main():
    """主处理流程"""
    old_data = merge_old_data(CONFIG['dates']['old'], CONFIG['columns'])
    new_data = pd.read_excel(f"数据/{CONFIG['dates']['new']}.xlsx", usecols = CONFIG['columns'])

    df = process_records(
        new_data=new_data,
        old_data=old_data,
        use_old_data=True,
        old_time_toll=CONFIG['dates']['old'],
        ranking_type=CONFIG['ranking_type']
    )
    df = df.loc[df.groupby('name')['point'].idxmax()].reset_index(drop=True)
    df = calculate_ranks(df)
    df = update_rank_and_rate(df, f"{CONFIG['output_paths']['total']}/{CONFIG['dates']['previous']}.xlsx")
    
    save_to_excel(df, f"{CONFIG['output_paths']['total']}/{CONFIG['dates']['target']}.xlsx")

    top_20_names = set(df[df['rank'] <= 20]['name'])
    new_songs_df = filter_new_songs(df, top_20_names)
    
    if not new_songs_df.empty:
        new_songs_df = calculate_ranks(new_songs_df)
        save_to_excel(new_songs_df, f"{CONFIG['output_paths']['new_song']}/新曲{CONFIG['dates']['target']}.xlsx")

if __name__ == "__main__":
    main()
