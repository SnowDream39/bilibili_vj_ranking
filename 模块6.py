#合并收录曲目

import pandas as pd

# 读取两个Excel文件
track_list_df = pd.read_excel('收录曲目.xlsx')
higher_than_5w_df = pd.read_excel('higher_than_5w.xlsx')

# 通过 BVID 进行合并，'收录曲目'优先
merged_df = pd.merge(higher_than_5w_df, track_list_df, on='BVID', how='outer', suffixes=('_higher', '_track'))

# 以"收录曲目"中的字段为准，更新字段
for column in track_list_df.columns:
    if column != 'BVID':  # 排除BVID列
        merged_df[column] = merged_df[f'{column}_track'].combine_first(merged_df[f'{column}_higher'])

# 删除多余的列
merged_df = merged_df[track_list_df.columns]

# 保存合并后的数据
merged_df.to_excel('merged_file.xlsx', index=False)
