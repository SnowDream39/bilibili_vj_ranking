import pandas as pd
import os
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter
from datetime import datetime, timedelta

def main():
    existed_song1 = '20240907'
    existed_song2 = '20240906'
    new_song1 = '新曲20240907'
    new_song2 = '新曲20240906'
    today_date = 20240906
    
    combined_df = read_and_combine_sheets(existed_song1, existed_song2, new_song1, new_song2)
    combined_df = calculate_ranks(combined_df)
    combined_df = format_r_columns(combined_df)
    combined_df = update_count(combined_df, today_date)
    
    save_to_excel(combined_df, f'差异/合并表格/{existed_song1}与{existed_song2}.xlsx')

    df3 = pd.read_excel(f'新曲数据/{new_song1}.xlsx')
    updated_df1 = process_new_songs(combined_df, df3, existed_song1)
    save_to_excel(updated_df1, f'数据/{existed_song1}.xlsx', adjust_width=False)

    updated_existing_songs = update_existing_songs(combined_df)
    save_to_excel(updated_existing_songs, '收录曲目.xlsx', adjust_width=False)
    
def read_and_combine_sheets(existed_song1, existed_song2, new_song1, new_song2):
    """合并新旧曲日增文件"""
    
    existed_song = f'{existed_song1}与{existed_song2}'
    new_song = f'{new_song1}与{new_song2}'

    df1 = pd.read_excel(f'差异/非新曲/{existed_song}.xlsx')
    df2 = pd.read_excel(f'差异/新曲/{new_song}.xlsx')

    combined_df = pd.concat([df1, df2]).drop_duplicates(subset=['bvid'], keep='last')
    return combined_df

def calculate_ranks(df):
    """重新计算各项排名"""
    df = df.sort_values('point', ascending=False)
    for col in ['view', 'favorite', 'coin', 'like']:
        df[f'{col}_rank'] = df[col].rank(ascending=False, method='min')
    df['rank'] = df['point'].rank(ascending=False, method='min')
    return df

def format_r_columns(df):
    """格式化各项补正数据"""
    
    r_columns = ['viewR', 'favoriteR', 'coinR', 'likeR']
    for col in r_columns:
        df[col] = df[col].apply(lambda x: f'{x:.2f}')
    return df

def update_count(df_today, today_date):
    """计算本期歌曲上榜次数"""
    
    prev_date = (datetime.strptime(str(today_date), '%Y%m%d') - timedelta(days=1)).strftime("%Y%m%d")
    prev_file_path = f'差异/合并表格/{today_date}与{prev_date}.xlsx'
    
    if os.path.exists(prev_file_path):
        df_prev = pd.read_excel(prev_file_path)
        if 'count' not in df_prev.columns:
            df_prev['count'] = 0  
    else:
        df_prev = pd.DataFrame(columns=['name', 'count'])

    prev_count_dict = dict(zip(df_prev['name'], df_prev['count']))
    
    df_today['count'] = df_today.apply(lambda row: prev_count_dict.get(row['name'], 0) + (1 if row['rank'] <= 20 else 0), axis=1)
    
    return df_today


def save_to_excel(df, filename, adjust_width=True):
    """格式化保存文件"""
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # 将pubdate列转换为字符串并格式化为"yyyy-mm-dd hh:mm:sss"
        if 'pubdate' in df.columns:
            df['pubdate'] = pd.to_datetime(df['pubdate'], errors='coerce')
            df['pubdate'] = df['pubdate'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(x) else '')
            df['pubdate'] = df['pubdate'].astype(str) 

        df.to_excel(writer, index=False, sheet_name='Sheet1')

        worksheet = writer.sheets['Sheet1']
        if 'pubdate' in df.columns:
            pubdate_col_idx = df.columns.get_loc('pubdate') + 1  # get_loc is 0-based, columns are 1-based
            pubdate_col_letter = get_column_letter(pubdate_col_idx)
            for cell in worksheet[pubdate_col_letter]:
                cell.alignment = Alignment(horizontal='left')


        if adjust_width:
            for i, col in enumerate(df.columns, 1):
                max_length = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.column_dimensions[get_column_letter(i)].width = max_length

    print(f"处理完成，{filename}数据已保存。")

def process_new_songs(df2, df3, existed_song1):
    """将本期新曲数据复制到旧曲数据"""
    
    new_songs = []
    for _, row in df2.iterrows():
        bvid = row['bvid']
        matching_row = df3[df3['bvid'] == bvid]
        if not matching_row.empty:
            match = matching_row.iloc[0]
            new_songs.append({
                'video_title': match['title'],
                'bvid': bvid,
                'title': row['name'],
                'author': row['author'],
                'uploader': row['uploader'],
                'copyright': row['copyright'],
                'synthesizer': row['synthesizer'],
                'vocal': row['vocal'],
                'type': row['type'],
                'pubdate': match['pubdate'],
                'duration':match['duration'],
                'view': match['view'],
                'favorite': match['favorite'],
                'coin': match['coin'],
                'like': match['like'],
                'image_url': match['image_url']
            })
    new_songs_df = pd.DataFrame(new_songs)
    existing_df1 = pd.read_excel(f'数据/{existed_song1}.xlsx', engine='openpyxl')
    updated_df1 = pd.concat([existing_df1, new_songs_df]).drop_duplicates(subset=['bvid'], keep='last')
    return updated_df1

def update_existing_songs(df2):
    selected_columns = ['name', 'bvid', 'title', 'view', 'pubdate', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal','type', 'image_url']
    df2_selected = df2[selected_columns]
    df2_selected.columns = ['Title', 'BVID', 'Video Title', 'View', 'Pubdate', 'Author', 'Uploader', 'Copyright', 'Synthesizer', 'Vocal', 'Type', 'image_url']

    existing_df = pd.read_excel('收录曲目.xlsx')
    new_entries = df2_selected[~df2_selected['BVID'].isin(existing_df['BVID'])]
    updated_df = pd.concat([existing_df, new_entries])
    return updated_df

if __name__ == "__main__":
    main()
