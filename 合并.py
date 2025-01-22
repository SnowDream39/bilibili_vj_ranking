import pandas as pd
import os
from openpyxl.utils import get_column_letter
from datetime import datetime, timedelta

def main():
    today_date = 20250120  # 旧曲日期

    old_time_toll = datetime.strptime(str(today_date), '%Y%m%d').strftime('%Y%m%d')
    new_time_toll = (datetime.strptime(str(today_date), '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
    old_time_new = f'新曲{old_time_toll}'
    new_time_new = f'新曲{new_time_toll}'

    combined_df = process_combined_data(new_time_toll, old_time_toll, new_time_new, old_time_new, today_date)
    save_to_excel(combined_df, f'差异/合并表格/{new_time_toll}与{old_time_toll}.xlsx')

    df2 = pd.read_excel(f'数据/{new_time_toll}.xlsx')
    df3 = pd.read_excel(f'新曲数据/{new_time_new}.xlsx')

    updated_existing_songs = update_existing_songs(combined_df)
    save_to_excel(updated_existing_songs, '收录曲目.xlsx', adjust_width=False)

    updated_df1 = process_new_songs(df2, df3, new_time_toll)
    save_to_excel(updated_df1, f'数据/{new_time_toll}.xlsx', adjust_width=False)

def process_combined_data(new_time_toll, old_time_toll, new_time_new, old_time_new, today_date):
    combined_df = read_and_combine_sheets(new_time_toll, old_time_toll, new_time_new, old_time_new)
    combined_df = calculate_ranks(combined_df)
    combined_df = format_columns(combined_df)
    combined_df = update_count(combined_df, today_date)
    combined_df = update_rank_and_rate(combined_df, today_date)
    combined_df = calculate_ranks(combined_df)
    return combined_df

def read_and_combine_sheets(new_time_toll, old_time_toll, new_time_new, old_time_new):
    df1 = pd.read_excel(f'差异/非新曲/{new_time_toll}与{old_time_toll}.xlsx')
    df2 = pd.read_excel(f'差异/新曲/{new_time_new}与{old_time_new}.xlsx')
    combined_df = pd.concat([df1, df2]).drop_duplicates(subset=['bvid'], keep='last')
    return merge_duplicate_names(combined_df)

def calculate_ranks(df):
    df = df.sort_values('point', ascending=False)
    for col in ['view', 'favorite', 'coin', 'like']:
        df[f'{col}_rank'] = df[col].rank(ascending=False, method='min')
    df['rank'] = df['point'].rank(ascending=False, method='min')
    return df

def merge_duplicate_names(df):
    merged_df = pd.DataFrame()
    grouped = df.groupby('name')
    
    for name, group in grouped:
        if len(group) > 1:
            best_record = group.loc[group['point'].idxmax()].copy() 
            merged_df = pd.concat([merged_df, best_record.to_frame().T])
        else: merged_df = pd.concat([merged_df, group])
    return merged_df

def format_columns(df):
    columns = ['viewR', 'favoriteR', 'coinR', 'likeR', 'fixA', 'fixB', 'fixC']
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: f'{x:.2f}')
    return df

def update_count(df_today, today_date):
    prev_date = (datetime.strptime(str(today_date), '%Y%m%d') - timedelta(days=1)).strftime("%Y%m%d")
    prev_file_path = f'差异/合并表格/{today_date}与{prev_date}.xlsx'

    if os.path.exists(prev_file_path):
        df_prev = pd.read_excel(prev_file_path)
    else:
        df_prev = pd.DataFrame(columns=['name', 'count'])

    prev_count_dict = df_prev.set_index('name')['count'].to_dict()
    df_today['count'] = df_today['name'].map(lambda x: prev_count_dict.get(x, 0)) + (df_today['rank'] <= 20).astype(int)

    return df_today

def update_rank_and_rate(df_today, today_date):
    prev_date = (datetime.strptime(str(today_date), '%Y%m%d') - timedelta(days=1)).strftime("%Y%m%d")
    prev_file_path = f'差异/合并表格/{today_date}与{prev_date}.xlsx'

    if os.path.exists(prev_file_path): df_prev = pd.read_excel(prev_file_path)
    else: df_prev = pd.DataFrame(columns=['name', 'rank', 'point'])

    prev_dict = df_prev.set_index('name')[['rank', 'point']].to_dict(orient='index')

    df_today['rank_before'] = df_today['name'].map(lambda x: prev_dict.get(x, {}).get('rank', '-'))
    df_today['point_before'] = df_today['name'].map(lambda x: prev_dict.get(x, {}).get('point', '-'))
    df_today['rate'] = df_today.apply(lambda row: calculate_rate(row['point'], row['point_before']), axis=1)

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

def process_new_songs(df2, df3, existed_song1):
    existing_df = pd.read_excel('收录曲目.xlsx')
    merged_df = df3.merge(df2, on='bvid', suffixes=('', '_y'))
    new_songs_df = merged_df[[
        'title', 'bvid', 'name', 'author', 'uploader', 
        'copyright', 'synthesizer', 'vocal', 'type', 
        'pubdate', 'duration', 'page', 'view', 
        'favorite', 'coin', 'like', 'image_url'
    ]]
    merged_df = df3.merge(existing_df, on='bvid', suffixes=('', '_y'))
    new_songs_df = merged_df[[
        'title', 'bvid', 'name_y', 'author_y', 'uploader', 
        'copyright_y', 'synthesizer_y', 'vocal_y', 'type_y', 
        'pubdate', 'duration', 'page', 'view', 
        'favorite', 'coin', 'like', 'image_url'
    ]].rename(columns={'name_y': 'name', 'author_y': 'author', 'copyright_y': 'copyright', 'synthesizer_y': 'synthesizer', 'vocal_y': 'vocal', 'type_y': 'type'})
    existing_df1 = pd.read_excel(f'数据/{existed_song1}.xlsx', engine='openpyxl')
    updated_df1 = pd.concat([existing_df1, new_songs_df]).drop_duplicates(subset=['bvid'], keep='last')
    return updated_df1

def update_existing_songs(df2):
    selected_columns = ['name', 'bvid', 'title', 'view', 'pubdate', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'type', 'image_url']
    df2_selected = df2[selected_columns]
    existing_df = pd.read_excel('收录曲目.xlsx')
    return pd.concat([existing_df, df2_selected[~df2_selected['bvid'].isin(existing_df['bvid'])]])

if __name__ == "__main__":
    main()
