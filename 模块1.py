#如果前一天的原始数据没了, 使用此程序
import pandas as pd

# 读取前一天的总数据和日增数据
total_data_yesterday = pd.read_excel(r'E:\Programming\python\bilibili日V周刊\数据\20240809000009.xlsx')
daily_increment = pd.read_excel(r'E:\Programming\python\bilibili日V周刊\差异\合并表格\20240810与20240809.xlsx')

# 合并数据，使用 bvid 作为键，保留所有日增数据中的记录
merged_data = pd.merge(total_data_yesterday, daily_increment, how='right', left_on='bvid', right_on='bvid')

# 检查合并后的列名
print("Merged columns:", merged_data.columns)

# 对指定的列进行加法操作，如果前一天的数据缺失，用0代替
for column in ['view', 'favorite', 'coin', 'like']:
    if f'{column}_x' in merged_data.columns and f'{column}_y' in merged_data.columns:
        merged_data[column] = merged_data[f'{column}_x'].fillna(0) + merged_data[f'{column}_y'].fillna(0)
    else:
        # 如果列名没有发生变化，直接复制
        merged_data[column] = merged_data[column].fillna(0)

# 如果前一天没有数据，使用日增数据中的信息填充其余字段
for column in ['video_title', 'title', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'pubdate', 'duration']:
    if f'{column}_x' in merged_data.columns and f'{column}_y' in merged_data.columns:
        merged_data[column] = merged_data[f'{column}_x'].combine_first(merged_data[f'{column}_y'])
    else:
        # 如果列名没有发生变化，直接复制
        merged_data[column] = merged_data[column]

# 选择今天的总数据中需要保留的列，使用 total_data_yesterday 的表头
final_columns = ['video_title', 'bvid', 'title', 'author', 'uploader', 'copyright', 'synthesizer', 'vocal', 'pubdate', 'duration', 'view', 'favorite', 'coin', 'like']
today_total_data = merged_data[final_columns]

# 保存今天的总数据
today_total_data.to_excel(r'E:\Programming\python\bilibili日V周刊\数据\20240810_total_data.xlsx', index=False)
print("今天的总数据已经保存到 'E:\Programming\python\bilibili日V周刊\数据\20240810_total_data.xlsx'")
