import pandas as pd
from datetime import datetime, timedelta

# modes = ["any", "week"]

mode = 0
date2 = "20250303"

if mode == 0: 
    date1 = (datetime.strptime(date2, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
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

def record_view_change(date1, date2):
    file_date1 = f"数据/{date1}.xlsx"
    file_date2 = f"数据/{date2}.xlsx"
    
    df_date1 = pd.read_excel(file_date1)
    df_date2 = pd.read_excel(file_date2)
    
    df_merged = pd.merge(df_date1[['bvid', 'view']], 
                         df_date2[['bvid', 'view', 'title', 'name', 'author', 'pubdate']], 
                         on='bvid', how='right', suffixes=(f'_{date1}', f'_{date2}'))
    
    df_merged[f'view_{date1}'] = df_merged[f'view_{date1}'].fillna(0)
    
    df_merged[f'view_{date1}_million'] = (df_merged[f'view_{date1}'] // 1000000).astype(int)
    df_merged[f'view_{date2}_million'] = (df_merged[f'view_{date2}'] // 1000000).astype(int)
    df_merged[f'view_{date1}_10w'] = (df_merged[f'view_{date1}'] // 100000).astype(int)
    df_merged[f'view_{date2}_10w'] = (df_merged[f'view_{date2}'] // 100000).astype(int)
    
    df_merged['pubdate'] = pd.to_datetime(df_merged['pubdate'], format='%Y-%m-%d %H:%M:%S')
    
    date1_str = f"{date1[:4]}-{date1[4:6]}-{date1[6:]}"  
    df_merged['is_new_video'] = df_merged['pubdate'] > pd.to_datetime(date1_str)
    
    million_rows = []
    _10w_rows = []

    for _, row in df_merged.iterrows():
        view1_million = row[f'view_{date1}_million']
        view2_million = row[f'view_{date2}_million']
        view1_10w = row[f'view_{date1}_10w']
        view2_10w = row[f'view_{date2}_10w']
        
        if not row['is_new_video'] and row[f'view_{date1}'] == 0:
            continue

        # 百万级变化记录
        if row['is_new_video']:
            if view2_million > 0:
                for million in range(1, view2_million + 1):
                    million_rows.append({
                        'title': row['title'],
                        'bvid': row['bvid'],
                        'name': row['name'],
                        'author': row['author'],
                        'pubdate': row['pubdate'],
                        'million_crossed': million 
                    })
        else:
            if view2_million > view1_million:
                for million in range(view1_million + 1, view2_million + 1):
                    million_rows.append({
                        'title': row['title'],
                        'bvid': row['bvid'],
                        'name': row['name'],
                        'author': row['author'],
                        'pubdate': row['pubdate'],
                        'million_crossed': million  
                    })
        
        # 10万级变化记录
        if row['is_new_video']:
            if view2_10w > 0 and view2_10w % 10 == 1:  # 确保不包括20万、30万等整10万倍数
                _10w_rows.append({
                    'title': row['title'],
                    'bvid': row['bvid'],
                    'name': row['name'],
                    'author': row['author'],
                    'pubdate': row['pubdate'],
                    '10w_crossed': view2_10w * 10
                })
        else:
            if view2_10w > view1_10w:
                for _10w in range(view1_10w + 1, view2_10w + 1):
                    _10nw=[]
                    if _10w == 1:  # 记录非整10万倍的10万变化
                        _10w_rows.append({
                            'title': row['title'],
                            'bvid': row['bvid'],
                            'name': row['name'],
                            'author': row['author'],
                            'pubdate': row['pubdate'],
                            '10w_crossed': _10w * 10
                        })
                        
                    else:
                        if _10w in [2,3,4,5,6,7,8,9,25, 95,75, 99]:
                            _10nw.append({
                                'name': row['name'],
                                '10w_crossed': _10w * 10,
                                'bvid': row['bvid']
                            })
                            for _10nw_row in _10nw:
                                print(f"{_10nw_row['10w_crossed']}万：{_10nw_row['name']}  {_10nw_row['bvid']}")
    # 检查是否有百万记录
    if million_rows:
        df_million_export = pd.DataFrame(million_rows)
        df_million_export = df_million_export.sort_values(by='million_crossed', ascending=False)
        # 输出百万记录到Excel
        million_output_file = f"整数播放达成/百万/百万记录{date2}与{date1}.xlsx" if mode == 0 else f"整数播放达成/百万/百万记录{date2[:4]}-{date2[4:6]}-{date2[6:]}.xlsx"
        with pd.ExcelWriter(million_output_file, engine='openpyxl') as writer:
            df_million_export.to_excel(writer, index=False, sheet_name='Sheet1')
            adjust_column_width(writer, 'Sheet1')
        
        # 打印百万播放记录
        for _, row in df_million_export.iterrows():
            print(f"{row['million_crossed'] * 100}万：{row['name']}   {row['bvid']}")

    # 检查是否有10万记录
    if _10w_rows:
        df_10w_export = pd.DataFrame(_10w_rows)
        df_10w_export = df_10w_export.sort_values(by='10w_crossed', ascending=False)
        # 输出10万记录到Excel
        _10w_output_file = f"整数播放达成/十万/十万记录{date2}与{date1}.xlsx" if mode == 0 else f"整数播放达成/十万/十万记录{date2[:4]}-{date2[4:6]}-{date2[6:]}.xlsx"
        with pd.ExcelWriter(_10w_output_file, engine='openpyxl') as writer:
            df_10w_export.to_excel(writer, index=False, sheet_name='Sheet1')
            adjust_column_width(writer, 'Sheet1')
        
        # 打印10万播放记录
        for _, row in df_10w_export.iterrows():
            print(f"{row['10w_crossed']}万：{row['name']}   {row['bvid']}")

if __name__ == "__main__":
    record_view_change(date1, date2)
