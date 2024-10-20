import pandas as pd
from datetime import datetime, timedelta

def main():
    today = 20241019
    now_time_data = datetime.strptime(str(today), '%Y%m%d').strftime('%Y%m%d')
    new_time_data = (datetime.strptime(str(today), '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
    old_time_data = (datetime.strptime(str(today), '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')
    
    file_path = f'E:/Programming/python/bilibili日V周刊/差异/新曲/新曲{new_time_data}与新曲{now_time_data}.xlsx'
    previous_rank_path = f'E:/Programming/python/bilibili日V周刊/新曲榜/新曲榜{now_time_data}与{old_time_data}.xlsx'
    output_path = f'新曲榜/新曲榜{new_time_data}与{now_time_data}.xlsx'

    df, previous_rank_df = load_data(file_path, previous_rank_path)
    df = df[['title', 'bvid', 'name', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 'page', 'view', 'favorite', 'coin', 'like', 'viewR', 'favoriteR', 'coinR', 'likeR', 'point', 'image_url']]
    previous_rank_df = previous_rank_df[['name', 'highest_rank']]

    df = filter_recent_songs(df, now_time_data)
    new_ranking_df = calculate_rankings(df, previous_rank_df)
    new_ranking_df = add_rank_columns(new_ranking_df)

    final_ranking = new_ranking_df[['title', 'bvid', 'name', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 'page', 'view', 'favorite', 'coin', 'like', 'viewR', 'favoriteR', 'coinR', 'likeR', 'point', 'image_url', 'view_rank', 'favorite_rank', 'coin_rank', 'like_rank', 'rank', 'highest_rank']]
    final_ranking = format_columns(final_ranking)
    save_to_excel(final_ranking, output_path)

def load_data(file_path, previous_rank_path):
    xls = pd.ExcelFile(file_path)
    df = pd.read_excel(xls, 'Sheet1')
    previous_rank_df = pd.read_excel(previous_rank_path)
    return df, previous_rank_df

def filter_recent_songs(df, today_date):
    if df['pubdate'].dtype == 'O': 
        df['pubdate'] = pd.to_datetime(df['pubdate'])
    
    start_date = today_date - timedelta(days=1)
    end_date = today_date + timedelta(days=1)
    return df[(df['pubdate'] >= start_date) & (df['pubdate'] < end_date)]

def calculate_rankings(df, previous_rank_df):
    df = df.sort_values(by='point', ascending=False).reset_index(drop=True)
    df['rank'] = df.index + 1

    merged_df = df.merge(previous_rank_df, on='name', how='left')
    merged_df['highest_rank'] = merged_df['highest_rank'].fillna(float('inf'))

    new_ranking = []
    ignore_rank = 0

    for _, row in merged_df.iterrows():
        if ((row['rank'] - ignore_rank) < row['highest_rank']):
            row['highest_rank'] = row['rank'] - ignore_rank
            row['rank'] = row['rank'] - ignore_rank
            new_ranking.append(row)
        else:
            ignore_rank += 1
    
    return pd.DataFrame(new_ranking).sort_values(by='rank').reset_index(drop=True)

def add_rank_columns(df):
    df['view_rank'] = df['view'].rank(ascending=False, method='min').astype(int)
    df['favorite_rank'] = df['favorite'].rank(ascending=False, method='min').astype(int)
    df['coin_rank'] = df['coin'].rank(ascending=False, method='min').astype(int)
    df['like_rank'] = df['like'].rank(ascending=False, method='min').astype(int)
    return df

def format_columns(df):
    pd.options.display.float_format = '{:.2f}'.format
    for col in ['viewR', 'favoriteR', 'coinR', 'likeR']:
        df[col] = df[col].map('{:.2f}'.format)
    return df

def save_to_excel(df, output_path):
    if 'pubdate' in df.columns:
        df['pubdate'] = pd.to_datetime(df['pubdate'], errors='coerce')
        df['pubdate'] = df['pubdate'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(x) else '')
        df['pubdate'] = df['pubdate'].astype(str)  
    df.to_excel(output_path, index=False)
    print("处理完成，详细数据已保存到", output_path)

if __name__ == "__main__":
    main()
