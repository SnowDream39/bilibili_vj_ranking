import pandas as pd
from datetime import datetime, timedelta

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

if __name__ == "__main__":
    today = datetime.now()
    day_of_week = today.weekday()
    modes_to_run = []
    if day_of_week == 5:  #星期六
        modes_to_run = [0, 1]  
    else:
        modes_to_run = [0]

    for mode in modes_to_run:
        
        date2 = (datetime.now()).replace(hour=0, minute=0,second=0,microsecond=0).strftime('%Y%m%d')

        if mode == 0: 
            date1 = (datetime.strptime(date2, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
            print("\n--- 正在执行日对比 ---")
        else: 
            date1 = (datetime.strptime(date2, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
            print("\n--- 正在执行周对比 ---")

        file_date1 = f"数据/{date1}.xlsx"
        file_date2 = f"数据/{date2}.xlsx"
        
        try:
            df_date1 = pd.read_excel(file_date1)
            df_date2 = pd.read_excel(file_date2)
        except FileNotFoundError as e:
            print(f"错误：找不到文件 {e.filename}")
            continue 
        
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
            
            if row['is_new_video']:
                if view2_10w > 0 and view2_10w % 10 == 1:
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
                        if _10w == 1:
                            _10w_rows.append({
                                'title': row['title'],
                                'bvid': row['bvid'],
                                'name': row['name'],
                                'author': row['author'],
                                'pubdate': row['pubdate'],
                                '10w_crossed': _10w * 10
                            })
                            
                        else:
                            if _10w in [2,3,4,5,6,7,8,9,25, 95, 75, 99]:
                                _10nw.append({
                                    'name': row['name'],
                                    '10w_crossed': _10w * 10,
                                    'bvid': row['bvid']
                                })
                                for _10nw_row in _10nw:
                                    print(f"{_10nw_row['10w_crossed']}万：{_10nw_row['name']}  {_10nw_row['bvid']}")

        if million_rows:
            df_million_export = pd.DataFrame(million_rows)
            df_million_export = df_million_export.sort_values(by='million_crossed', ascending=False)
            million_output_file = f"整数播放达成/百万/百万记录{date2}与{date1}.xlsx" if mode == 0 else f"整数播放达成/百万/百万记录{date2[:4]}-{date2[4:6]}-{date2[6:]}.xlsx"
            with pd.ExcelWriter(million_output_file, engine='openpyxl') as writer:
                df_million_export.to_excel(writer, index=False, sheet_name='Sheet1')
                adjust_column_width(writer, 'Sheet1')
            
            for _, row in df_million_export.iterrows():
                print(f"{row['million_crossed'] * 100}万：{row['name']}   {row['bvid']}")

        if _10w_rows:
            df_10w_export = pd.DataFrame(_10w_rows)
            df_10w_export = df_10w_export.sort_values(by='10w_crossed', ascending=False)
            _10w_output_file = f"整数播放达成/十万/十万记录{date2}与{date1}.xlsx" if mode == 0 else f"整数播放达成/十万/十万记录{date2[:4]}-{date2[4:6]}-{date2[6:]}.xlsx"
            with pd.ExcelWriter(_10w_output_file, engine='openpyxl') as writer:
                df_10w_export.to_excel(writer, index=False, sheet_name='Sheet1')
                adjust_column_width(writer, 'Sheet1')
            
            for _, row in df_10w_export.iterrows():
                print(f"{row['10w_crossed']}万：{row['name']}   {row['bvid']}")
        
    a = input()
