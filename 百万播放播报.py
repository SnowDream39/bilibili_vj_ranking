import pandas as pd
from datetime import datetime, timedelta

# modes = ["any", "week"]

mode = 1
date2 = "20241102"

if mode == 0: 
    date1 = "20241031"
else: 
    date1 = (datetime.strptime(date2, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")

def adjust_column_width(writer, sheet_name):
    worksheet = writer.sheets[sheet_name]
    for col in worksheet.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except:
                pass
        adjusted_width = (max_length + 2)
        worksheet.column_dimensions[column].width = adjusted_width

def record_million_view_change(date1, date2):
    file_date1 = f"数据/{date1}.xlsx"
    file_date2 = f"数据/{date2}.xlsx"
    
    df_date1 = pd.read_excel(file_date1)
    df_date2 = pd.read_excel(file_date2)
    
    df_merged = pd.merge(df_date1[['bvid', 'view']], 
                         df_date2[['bvid', 'view', 'video_title', 'title', 'author', 'pubdate']], 
                         on='bvid', how='right', suffixes=(f'_{date1}', f'_{date2}'))
    
    df_merged[f'view_{date1}'] = df_merged[f'view_{date1}'].fillna(0)
    
    df_merged[f'view_{date1}_million'] = (df_merged[f'view_{date1}'] // 1000000).astype(int)
    df_merged[f'view_{date2}_million'] = (df_merged[f'view_{date2}'] // 1000000).astype(int)
    
    df_merged['pubdate'] = pd.to_datetime(df_merged['pubdate'], format='%Y-%m-%d %H:%M:%S')
    
    date1_str = f"{date1[:4]}-{date1[4:6]}-{date1[6:]}"  
    df_merged['is_new_video'] = df_merged['pubdate'] > pd.to_datetime(date1_str)
    
    result_rows = []

    for _, row in df_merged.iterrows():
        view1_million = row[f'view_{date1}_million']
        view2_million = row[f'view_{date2}_million']
        
        if not row['is_new_video'] and row[f'view_{date1}'] == 0:
            continue

        if row['is_new_video']:
            if view2_million > 0:
                for million in range(1, view2_million + 1):
                    result_rows.append({
                        'video_title': row['video_title'],
                        'bvid': row['bvid'],
                        'title': row['title'],
                        'author': row['author'],
                        'pubdate': row['pubdate'],
                        'million_crossed': million 
                    })
        else:
            if view2_million > view1_million:
                for million in range(view1_million + 1, view2_million + 1):
                    result_rows.append({
                        'video_title': row['video_title'],
                        'bvid': row['bvid'],
                        'title': row['title'],
                        'author': row['author'],
                        'pubdate': row['pubdate'],
                        'million_crossed': million  
                    })

    df_export = pd.DataFrame(result_rows)
    
    df_export = df_export.sort_values(by='million_crossed', ascending=False)
    

    if mode == 0:
        output_file = f"百万/百万记录{date2}与{date1}.xlsx"
    else:
        output_file = f"百万/百万记录{date2[:4]}-{date2[4:6]}-{date2[6:]}.xlsx"
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df_export.to_excel(writer, index=False, sheet_name='百万播放记录')
        adjust_column_width(writer, '百万播放记录')

    print(f"百万播放变化记录已导出至 {output_file}")

if __name__ == "__main__":
    record_million_view_change(date1, date2)
