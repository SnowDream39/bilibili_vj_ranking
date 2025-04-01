import pandas as pd
from datetime import datetime, timedelta
from utils.io_utils import save_to_excel
from utils.calculator import calculate_ranks

def main():
    today = (datetime.now()-timedelta(days=1)).replace(hour=0, minute=0,second=0,microsecond=0).strftime('%Y%m%d')
    now_time_data = datetime.strptime(str(today), '%Y%m%d').strftime('%Y%m%d')
    new_time_data = (datetime.strptime(str(today), '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
    old_time_data = (datetime.strptime(str(today), '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')

    file_path = f'差异/新曲/新曲{new_time_data}与新曲{now_time_data}.xlsx'
    previous_rank_path = f'新曲榜/新曲榜{now_time_data}与{old_time_data}.xlsx'
    output_path = f'新曲榜/新曲榜{new_time_data}与{now_time_data}.xlsx'

    new_ranking_df = pd.read_excel(file_path)
    previous_ranking_df = pd.read_excel(previous_rank_path)
    columns = ['title', 'bvid', 'name', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 'page', 'view', 'favorite', 'coin', 'like', 'viewR', 'favoriteR', 'coinR', 'likeR', 'fixA', 'fixB', 'fixC', 'point', 'image_url']
    new_ranking_df = new_ranking_df[columns]
    previous_ranking_df = previous_ranking_df[['name', 'rank']]

    new_ranking_df = new_ranking_df.loc[new_ranking_df.groupby('name')['point'].idxmax()].reset_index(drop=True)
    new_ranking_df = filter_new_song(new_ranking_df, previous_ranking_df)
    new_ranking_df = calculate_ranks(new_ranking_df)
    save_to_excel(new_ranking_df, output_path)

def filter_new_song(df, previous_rank_df):
    df = df.sort_values(by='point', ascending=False).reset_index(drop=True)
    df['rank'] = df.index + 1
    df = df.merge(previous_rank_df[['name', 'rank']], on='name', how='left', suffixes=('', '_previous'))
    df['rank_previous'] = df['rank_previous'].fillna(1000)
    new_ranking = []
    ignore_rank = 0
    [(row.update({'rank_previous': row['rank']-ignore_rank, 'rank': row['rank']-ignore_rank}) or new_ranking.append(row) ) if (row['rank']-ignore_rank) < row['rank_previous'] else (ignore_rank := ignore_rank+1) for _, row in df.iterrows() ]
    return pd.DataFrame(new_ranking).sort_values(by='rank').reset_index(drop=True)

if __name__ == "__main__":
    main()
