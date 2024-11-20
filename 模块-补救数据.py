# 如果前一天的原始数据没了, 使用此程序
# 年久失修
import pandas as pd

# 读取前一天的总数据和日增数据
total_data_yesterday = pd.read_excel(r'E:\Programming\python\bilibili日V周刊\数据\20240809000009.xlsx')
daily_increment = pd.read_excel(r'E:\Programming\python\bilibili日V周刊\差异\合并表格\20240810与20240809.xlsx')

merged_data = pd.merge(total_data_yesterday, daily_increment, how='right', left_on='bvid', right_on='bvid')

print("Merged columns:", merged_data.columns)

for column in ['view', 'favorite', 'coin', 'like']:
    if f'{column}_x' in merged_data.columns and f'{column}_y' in merged_data.columns:
        merged_data[column] = merged_data[f'{column}_x'].fillna(0) + merged_data[f'{column}_y'].fillna(0)
    else:
   
        merged_data[column] = merged_data[column].fillna(0)


for column in ['video_title', 'title', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'pubdate', 'duration']:
    if f'{column}_x' in merged_data.columns and f'{column}_y' in merged_data.columns:
        merged_data[column] = merged_data[f'{column}_x'].combine_first(merged_data[f'{column}_y'])
    else:
        
        merged_data[column] = merged_data[column]


final_columns = ['video_title', 'bvid', 'title', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'pubdate', 'duration', 'view', 'favorite', 'coin', 'like']
today_total_data = merged_data[final_columns]

today_total_data.to_excel(r'E:\Programming\python\bilibili日V周刊\数据\20240810_total_data.xlsx', index=False)
print("今天的总数据已经保存到 'E:\Programming\python\bilibili日V周刊\数据\20240810_total_data.xlsx'")
