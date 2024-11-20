#合并收录曲目

import pandas as pd

track_list_df = pd.read_excel('收录曲目.xlsx')
priored_list_df = pd.read_excel('已处理10.xlsx')

# 通过 BVID 进行合并，以priored_list_df为准
merged_df = pd.merge(priored_list_df, track_list_df, on='BVID', how='outer', suffixes=('_priored', '_track'))

# 以"收录曲目"中的字段为准，更新字段
for column in track_list_df.columns:
    if column != 'BVID':  
        merged_df[column] = merged_df[f'{column}_track'].combine_first(merged_df[f'{column}_priored'])

merged_df = merged_df[track_list_df.columns]
merged_df.to_excel('新收录曲目.xlsx', index=False)
