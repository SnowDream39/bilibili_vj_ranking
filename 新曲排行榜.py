import pandas as pd

def main():
    today = 20240726
    file_path = 'E:/Programming/python/bilibili日V周刊/差异/新曲/新曲20240727000201与新曲20240726000653.xlsx'
    previous_rank_path = f'E:/Programming/python/bilibili日V周刊/新曲榜/新曲{today}与新曲{today-1}.xlsx'
    output_path = f'新曲榜/新曲{today+1}与新曲{today}.xlsx'

    df, previous_rank_df = load_data(today, file_path, previous_rank_path)
    df = df[['title', 'bvid', 'name', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'pubdate', 'duration', 'view', 'favorite', 'coin', 'like', 'viewR', 'favoriteR', 'coinR', 'likeR', 'point']]
    previous_rank_df = previous_rank_df[['title', 'highest_rank']]

    df = filter_recent_songs(df, today)
    new_ranking_df = calculate_rankings(df, previous_rank_df)
    new_ranking_df = add_rank_columns(new_ranking_df)

    final_ranking = new_ranking_df[['title', 'bvid', 'name', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'pubdate', 'duration', 'view', 'favorite', 'coin', 'like', 'viewR', 'favoriteR', 'coinR', 'likeR', 'point', 'view_rank', 'favorite_rank', 'coin_rank', 'like_rank', 'rank', 'highest_rank']]
    final_ranking = format_columns(final_ranking)
    save_to_excel(final_ranking, output_path)
    
def load_data(today, file_path, previous_rank_path):
    # 加载数据
    xls = pd.ExcelFile(file_path)
    df = pd.read_excel(xls, 'Sheet1')
    previous_rank_df = pd.read_excel(previous_rank_path)
    
    return df, previous_rank_df

def filter_recent_songs(df, today):
    # 检查 pubdate 列的类型，如果是字符串则进行转换
    if df['pubdate'].dtype == 'O': 
        df['pubdate'] = pd.to_datetime(df['pubdate'])
    
    # 只收录两天内的新曲
    start_date = pd.to_datetime(str(today - 1), format='%Y%m%d')
    end_date = pd.to_datetime(str(today + 1), format='%Y%m%d')
    return df[(df['pubdate'] >= start_date) & (df['pubdate'] < end_date)]

def calculate_rankings(df, previous_rank_df):
    # 计算新曲排行榜，只收录排名上升的歌曲
    df = df.sort_values(by='point', ascending=False).reset_index(drop=True)
    df['rank'] = df.index + 1

    merged_df = df.merge(previous_rank_df, on='title', how='left')
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
            df['pubdate'] = df['pubdate'].astype(str)  # 确保是字符串类型
    df.to_excel(output_path, index=False)
    print("处理完成，详细数据已保存到", output_path)

if __name__ == "__main__":
    main()
