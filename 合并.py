from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import yaml
import json
web_uploader = __import__('模块-上传网站')
from utils.io_utils import save_to_excel
from utils.calculator import calculate_ranks, update_rank_and_rate, update_count

def main():
    today_date = (datetime.now()-timedelta(days=1)).replace(hour=0, minute=0,second=0,microsecond=0).strftime('%Y%m%d')

    old_time_toll = datetime.strptime(str(today_date), '%Y%m%d').strftime('%Y%m%d')
    new_time_toll = (datetime.strptime(str(today_date), '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
    old_time_new = f'新曲{old_time_toll}'
    new_time_new = f'新曲{new_time_toll}'

    df = read_and_combine_sheets(new_time_toll, old_time_toll, new_time_new, old_time_new)
    updated_songs = update_existing_songs(df)
    save_to_excel(updated_songs, '收录曲目.xlsx')
    
    df = process_combined_data(df, today_date)
    save_to_excel(df, f'差异/合并表格/{new_time_toll}与{old_time_toll}.xlsx', json.load(Path('config/usecols.json').open(encoding='utf-8'))["columns"]["final_ranking"])

    df_new_toll = pd.read_excel(f'数据/{new_time_toll}.xlsx')
    df_new_new = pd.read_excel(f'新曲数据/{new_time_new}.xlsx')

    updated_data = process_new_songs(df_new_toll, df_new_new)
    save_to_excel(updated_data, f'数据/{new_time_toll}.xlsx')

def process_combined_data(df, today_date):
    df = merge_duplicate_names(df)
    df = calculate_ranks(df)
    df = update_count(df, f'差异/合并表格/{today_date}与{(datetime.strptime(str(today_date), "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")}.xlsx')
    df = update_rank_and_rate(df, f'差异/合并表格/{today_date}与{(datetime.strptime(str(today_date), "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")}.xlsx')
    return df

def read_and_combine_sheets(new_time_toll, old_time_toll, new_time_new, old_time_new):
    df_toll = pd.read_excel(f'差异/非新曲/{new_time_toll}与{old_time_toll}.xlsx')
    df_new = pd.read_excel(f'差异/新曲/{new_time_new}与{old_time_new}.xlsx')
    combined_df = pd.concat([df_toll, df_new]).drop_duplicates(subset=['bvid'], keep='last')
    return combined_df

def merge_duplicate_names(df):
    merged_df = pd.DataFrame()
    grouped = df.groupby('name')
    
    for _, group in grouped:
        if len(group) > 1:
            best_record = group.loc[group['point'].idxmax()].copy() 
            merged_df = pd.concat([merged_df, best_record.to_frame().T])
        else: merged_df = pd.concat([merged_df, group])
    return merged_df

def process_new_songs(df_new_toll, df_new_new):
    existing_df = pd.read_excel('收录曲目.xlsx')
    updated_df = df_new_new.merge(existing_df, on='bvid', suffixes=('', '_y'))
    df_new = updated_df[[
        'title', 'bvid', 'name_y', 'author_y', 'uploader', 
        'copyright_y', 'synthesizer_y', 'vocal_y', 'type_y', 
        'pubdate', 'duration', 'page', 'view', 
        'favorite', 'coin', 'like', 'image_url'
    ]].rename(columns={'name_y': 'name', 'author_y': 'author', 'copyright_y': 'copyright', 'synthesizer_y': 'synthesizer', 'vocal_y': 'vocal', 'type_y': 'type'})
    updated_df = pd.concat([df_new_toll, df_new]).drop_duplicates(subset=['bvid'], keep='last')
    return updated_df

def update_existing_songs(df):
    selected_columns = ['name', 'bvid', 'title', 'view', 'pubdate', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'type', 'image_url']
    df_selected = df[selected_columns]
    existing_df = pd.read_excel('收录曲目.xlsx')
    return pd.concat([existing_df, df_selected[~df_selected['bvid'].isin(existing_df['bvid'])]])

def upload():
    '''
    上传文件到数据服务器
    '''
    with open("config/上传数据服务器.yaml",encoding='utf-8') as file:
            data = yaml.safe_load(file)
            HOST = data['HOST']
            PORT = data['PORT']
            USERNAME = data['USERNAME']
            PASSWORD = data['PASSWORD']
            REMOTE_PATH = data['REMOTE_PATH']
            local_files = data['local_files']

    ssh = web_uploader.connect_ssh(HOST, PORT, USERNAME, PASSWORD)
    sftp = ssh.open_sftp()  
    web_uploader.upload_files(sftp, local_files, REMOTE_PATH)
    web_uploader.close_connections(sftp, ssh)

if __name__ == "__main__":
    main()
    upload()
    
