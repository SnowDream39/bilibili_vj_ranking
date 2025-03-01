import pandas as pd
from datetime import datetime, timedelta

def main():
    today = (datetime.now()-timedelta(days=1)).replace(hour=0, minute=0,second=0,microsecond=0).strftime('%Y%m%d')
    now_time_data = datetime.strptime(str(today), '%Y%m%d').strftime('%Y%m%d')
    new_time_data = (datetime.strptime(str(today), '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
    old_time_data = (datetime.strptime(str(today), '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')

    file_path = f'差异/新曲/新曲{new_time_data}与新曲{now_time_data}.xlsx'
    previous_rank_path = f'新曲榜/新曲榜{now_time_data}与{old_time_data}.xlsx'
    output_path = f'新曲榜/新曲榜{new_time_data}与{now_time_data}.xlsx'

    new_ranking_df, previous_ranking_df = load_data(file_path, previous_rank_path)
    columns = ['title', 'bvid', 'name', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 'page', 'view', 'favorite', 'coin', 'like', 'viewR', 'favoriteR', 'coinR', 'likeR', 'fixA', 'fixB', 'fixC', 'point', 'image_url']
    new_ranking_df = new_ranking_df[columns]
    previous_ranking_df = previous_ranking_df[['name', 'rank']]

    new_ranking_df = merge_duplicate_names(new_ranking_df)
    new_ranking_df = calculate_rankings(new_ranking_df, previous_ranking_df)
    new_ranking_df = add_rank_columns(new_ranking_df)
    new_ranking_df = new_ranking_df[columns + ['view_rank', 'favorite_rank', 'coin_rank', 'like_rank', 'rank']]
    new_ranking_df = format_columns(new_ranking_df)
    save_to_excel(new_ranking_df, output_path)

def load_data(file_path, previous_rank_path):
    xls = pd.ExcelFile(file_path)
    df = pd.read_excel(xls, 'Sheet1')
    previous_rank_df = pd.read_excel(previous_rank_path)
    return df, previous_rank_df

def merge_duplicate_names(df):
    df = df.sort_values(by=['name', 'point'], ascending=[True, False])
    df = df.drop_duplicates(subset='name', keep='first')
    return df

def calculate_rankings(df, previous_rank_df):
    df = df.sort_values(by='point', ascending=False).reset_index(drop=True)
    df['rank'] = df.index + 1
    df = df.merge(previous_rank_df[['name', 'rank']], on='name', how='left', suffixes=('', '_previous'))
    df['rank_previous'] = df['rank_previous'].fillna(1000)

    new_ranking = []
    ignore_rank = 0
    for _, row in df.iterrows():
        if (row['rank'] - ignore_rank) < row['rank_previous']:
            row['rank_previous'] = row['rank'] - ignore_rank
            row['rank'] = row['rank'] - ignore_rank
            new_ranking.append(row)
        else: ignore_rank += 1

    return pd.DataFrame(new_ranking).sort_values(by='rank').reset_index(drop=True)

def add_rank_columns(df):
    rank_columns = ['view', 'favorite', 'coin', 'like']
    for col in rank_columns:
        df[f'{col}_rank'] = df[col].rank(ascending=False, method='min').astype(int)
    return df

def format_columns(df):
    pd.options.display.float_format = '{:.2f}'.format
    for col in ['viewR', 'favoriteR', 'coinR', 'likeR','fixA', 'fixB', 'fixC']:
        df[col] = df[col].apply(lambda x: '{:.2f}'.format(x) if pd.notnull(x) else '')
    return df

def save_to_excel(df, output_path):
    df.to_excel(output_path, index=False)
    print(f'{output_path} 已保存')

if __name__ == "__main__":
    main()
