import pandas as pd
from datetime import datetime, timedelta

def main():
    file_path = '特殊/周刊新曲榜整理前.xlsx'

    output_path = f'新曲2024-09-06.xlsx'
    data = pd.read_excel(file_path)
    add_rank_columns(data)

    save_to_excel(data, output_path)

def load_data(today, file_path, previous_rank_path):
    xls = pd.ExcelFile(file_path)
    df = pd.read_excel(xls, 'Sheet1')
    previous_rank_df = pd.read_excel(previous_rank_path)
    return df, previous_rank_df

def filter_recent_songs(df, today_date):
    # 检查 pubdate 列的类型，如果是字符串则进行转换
    if df['pubdate'].dtype == 'O': 
        df['pubdate'] = pd.to_datetime(df['pubdate'])
    
    # 只收录两天内的新曲
    start_date = today_date - timedelta(days=1)
    end_date = today_date + timedelta(days=1)
    return df[(df['pubdate'] >= start_date) & (df['pubdate'] < end_date)]

def calculate_rankings(df, previous_rank_df):
    # 计算新曲排行榜，只收录排名上升的歌曲
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

