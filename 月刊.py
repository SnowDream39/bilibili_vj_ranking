import os
from openpyxl.utils import get_column_letter
import pandas as pd
from math import ceil, floor
from datetime import datetime
from dateutil.relativedelta import relativedelta

CONFIG = {
    "columns": [
        'title', 'bvid', 'name', 'author', 'uploader', 'copyright', 
        'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 'page', 
        'view', 'favorite', 'coin', 'like', 'image_url'
    ],
    "dates": {
        "old": '20241101',
        "new": '20241201',
        "target": '2024-11',
    },
    "output_paths": {
        "total": "月刊/总榜",
        "new_song": "月刊/新曲榜"
    }
}
CONFIG["dates"]["previous"] = (datetime.strptime('2024-10', '%Y-%m') - relativedelta(months=1)).strftime('%Y-%m')

def read_data(file_path, columns=None):
    return pd.read_excel(file_path, usecols=columns)

def calculate_differences(new, old):
    return {col: new[col] - old.get(col, 0) for col in ['view', 'favorite', 'coin', 'like']}

def calculate_scores(view, favorite, coin, like, copyright):
    copyright = 1 if copyright in [1, 3] else 2
    fixA = 0 if coin <= 0 else (1 if copyright == 1 else ceil(max(1, (view + 20 * favorite + 40 * coin + 10 * like) / (200 * coin)) * 100) / 100)
    fixB = 0 if view + 20 * favorite <=0 else ceil(min(1, 3 * (20 * coin + 10 * like) / (view + 20 * favorite)) * 100) / 100
    fixC = 0 if like + favorite <= 0 else ceil(min(1, (like + favorite + 20 * coin * fixA)/(2 * like + 2 * favorite)) * 100) / 100
    
    viewR = 0 if view <= 0 else max(ceil(min(max((fixA * coin + favorite), 0) * 25 / view, 1) * 100) / 100, 0)
    favoriteR = 0 if favorite <= 0 else max(ceil(min((favorite + 2 * fixA * coin) * 10 / (favorite * 15 + view) * 40, 20) * 100) / 100, 0)
    coinR = 0 if fixA * coin * 40 + view <= 0 else max(ceil(min((fixA * coin * 40) / (fixA * coin * 30 + view) * 80, 40) * 100) / 100, 0)
    likeR = 0 if like <= 0 else max(floor(min(5, max(fixA * coin + favorite, 0) / (like * 20 + view) * 100) * 100) / 100, 0)

    return viewR, favoriteR, coinR, likeR, fixA, fixB, fixC

def calculate_points(diff, scores):
    """计算总分"""
    viewR, favoriteR, coinR, likeR, fixA = scores[:5]
    viewP = diff['view'] * viewR
    favoriteP = diff['favorite'] * favoriteR
    coinP = diff['coin'] * coinR * fixA
    likeP = diff['like'] * likeR
    return viewP + favoriteP + coinP + likeP

def calculate_ranks(df):
    """重新计算各项排名"""
    df = df.sort_values('point', ascending=False)
    for col in ['view', 'favorite', 'coin', 'like']:
        df[f'{col}_rank'] = df[col].rank(ascending=False, method='min')
    df['rank'] = df['point'].rank(ascending=False, method='min')
    return df

def format_columns(df):
    """格式化补正数据列"""
    columns = ['viewR', 'favoriteR', 'coinR', 'likeR', 'fixA', 'fixB', 'fixC']
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].apply(lambda x: f'{x:.2f}' if pd.notnull(x) else '')
    return df

def update_rank_and_rate(df_today):
    prev_file_path = f"{CONFIG['output_paths']['total']}/{CONFIG['dates']['previous']}.xlsx"

    if os.path.exists(prev_file_path): df_prev = pd.read_excel(prev_file_path)
    else: df_prev = pd.DataFrame(columns=['name', 'rank', 'point'])
    
    df_prev = df_prev.sort_values('point', ascending=False).drop_duplicates(subset='name', keep='first')
    prev_dict = df_prev.set_index('name')[['rank', 'point']].to_dict(orient='index')

    df_today['rank_before'] = df_today['name'].map(lambda x: prev_dict.get(x, {}).get('rank', '-'))
    df_today['point_before'] = df_today['name'].map(lambda x: prev_dict.get(x, {}).get('point', '-'))
    df_today['rate'] = df_today.apply(lambda row: calculate_rate(row['point'], row['point_before']), axis=1)
    df_today = df_today.sort_values('point', ascending=False)
    return df_today

def calculate_rate(current_point, previous_point):
    if previous_point == '-': return 'NEW'
    if previous_point == 0:   return 'inf'
    return f"{(current_point - previous_point) / previous_point:.2%}"

def save_to_excel(df, filename, adjust_width=True):
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        if 'pubdate' in df.columns:
            df['pubdate'] = pd.to_datetime(df['pubdate'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
        df.to_excel(writer, index=False, sheet_name='Sheet1')

        worksheet = writer.sheets['Sheet1']
        if adjust_width:
            adjust_column_width(worksheet, df)
    print(f'{filename} 已保存')
    
def adjust_column_width(worksheet, df):
    for i, col in enumerate(df.columns, 1):
        max_length = max(df[col].astype(str).map(len).max(), len(col)) + 2
        worksheet.column_dimensions[get_column_letter(i)].width = max_length

def merge_old_data(date, columns):
    main_data = read_data(f"数据/{date}.xlsx", columns)
    new_song_file = f"新曲数据/新曲{date}.xlsx"
    if os.path.exists(new_song_file):
        new_song_data = read_data(new_song_file, columns)
        merged_data = pd.concat([main_data, new_song_data]).drop_duplicates(subset=['bvid'], keep='first')
        return merged_data
    else:
        return main_data
    
def process_records(new_data, old_data):
    """处理数据记录"""
    data_list = []
    for i in new_data.index:
        bvid = new_data.at[i, "bvid"]
        pubdate = str(new_data.at[i, 'pubdate'])

        try:
            new_record = new_data[new_data['bvid'] == bvid]
            old_record = old_data[old_data['bvid'] == bvid]

            new = new_record.iloc[0]
            if old_record.empty:
                if datetime.strptime(pubdate, "%Y-%m-%d %H:%M:%S") < datetime.strptime(CONFIG['dates']['old'], "%Y%m%d"):
                    continue
                old = {'view': 0, 'favorite': 0, 'coin': 0, 'like': 0}
            else:
                old = old_record.iloc[0]

            diff = calculate_differences(new, old)
            scores = calculate_scores(diff['view'], diff['favorite'], diff['coin'], diff['like'], new['copyright'])
            point = round(scores[5] * scores[6] * calculate_points(diff, scores))

            data_list.append({
                'title': new['title'], 'bvid': bvid, 'name': new['name'],
                'author': new['author'], 'uploader': new['uploader'], 'copyright': new['copyright'],
                'synthesizer': new['synthesizer'], 'vocal': new['vocal'],
                'type': new['type'], 'pubdate': pubdate, 'duration': new['duration'], 'page': new['page'],
                'view': diff['view'], 'favorite': diff['favorite'], 'coin': diff['coin'], 'like': diff['like'],
                'viewR': scores[0], 'favoriteR': scores[1], 'coinR': scores[2], 'likeR': scores[3],
                'fixA': scores[4], 'fixB': scores[5], 'fixC': scores[6], 'point': point, 'image_url': new['image_url']
            })
        except Exception as e:
            print(f"Error processing {bvid}: {e}")
            
    return pd.DataFrame(data_list)

def filter_new_songs(df, top_20_names):
    """筛选新曲"""
    start_date = datetime.strptime(CONFIG['dates']['old'], "%Y%m%d")
    end_date = datetime.strptime(CONFIG['dates']['new'], "%Y%m%d")
    
    df['pubdate'] = pd.to_datetime(df['pubdate'])
    mask = ((df['pubdate'] >= start_date) & (df['pubdate'] < end_date) & (~df['name'].isin(top_20_names)))
    return df[mask].copy()

def main_processing():
    """主处理流程"""
    old_data = merge_old_data(CONFIG['dates']['old'], CONFIG['columns'])
    new_data = read_data(f"数据/{CONFIG['dates']['new']}.xlsx", CONFIG['columns'])

    df = process_records(new_data, old_data)
    df = df.loc[df.groupby('name')['point'].idxmax()].reset_index(drop=True)
    df = calculate_ranks(df)
    df = format_columns(df)
    df = update_rank_and_rate(df)
    
    save_to_excel(df, f"{CONFIG['output_paths']['total']}/{CONFIG['dates']['target']}.xlsx")

    top_20_names = set(df[df['rank'] <= 20]['name'])
    new_songs_df = filter_new_songs(df, top_20_names)
    
    if not new_songs_df.empty:
        new_songs_df = calculate_ranks(new_songs_df)
        new_songs_df = format_columns(new_songs_df)
        save_to_excel(new_songs_df, f"{CONFIG['output_paths']['new_song']}/新曲{CONFIG['dates']['target']}.xlsx")

if __name__ == "__main__":
    main_processing()
