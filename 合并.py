import pandas as pd

existed_song = '20240630121343与20240630000302'
new_song = '新曲20240630121104与新曲20240630000030'

# 假设 df1 和 df2 是你读取的两个差异表格
df1 = pd.read_excel(f'差异/{existed_song}.xlsx')
df2 = pd.read_excel(f'差异/新曲/{new_song}.xlsx')

combined_df = pd.concat([df1, df2]).drop_duplicates(subset=['bvid'], keep='last')

# 重新计算排名
combined_df = combined_df.sort_values('point', ascending=False)
combined_df['view_rank'] = combined_df['view'].rank(ascending=False, method='min')
combined_df['favorite_rank'] = combined_df['favorite'].rank(ascending=False, method='min')
combined_df['coin_rank'] = combined_df['coin'].rank(ascending=False, method='min')
combined_df['like_rank'] = combined_df['like'].rank(ascending=False, method='min')

# 保存合并后的 DataFrame
filename = f'差异/合并表格/{existed_song}.xlsx'
writer = pd.ExcelWriter(filename, engine='openpyxl')
combined_df.to_excel(writer, index=False, sheet_name='Sheet1')

# 自动调整列宽
worksheet = writer.sheets['Sheet1']
for i, column_cells in enumerate(worksheet.columns):
    length = max(len(str(cell.value)) for cell in column_cells)
    worksheet.column_dimensions[worksheet.cell(row=1, column=i+1).column_letter].width = length + 2

writer.close() 

print("处理完成，合并后的数据已保存到", filename)

# 输出表格2的曲目信息到收录曲目.xlsx
selected_columns = ['name', 'bvid', 'title', 'view', 'pubdate', 'author', 'uploader', 'copyright']
df2_selected = df2[selected_columns]

df2_selected.columns = ['Title', 'BVID', 'Video Title', 'View', 'Pubdate', 'Author', 'Uploader', 'Copyright']

existing_df = pd.read_excel('收录曲目.xlsx')

existing_bvids = set(existing_df['BVID'])
df2_selected_unique = df2_selected[~df2_selected['BVID'].isin(existing_bvids)]

updated_df = pd.concat([existing_df, df2_selected_unique])

# 保存更新后的DataFrame到收录曲目.xlsx
output_filename = '收录曲目.xlsx'
writer2 = pd.ExcelWriter(output_filename, engine='openpyxl')
updated_df.to_excel(writer2, index=False, sheet_name='Sheet1')

writer2.close() 

print("处理完成，表格2的曲目信息已保存到", output_filename)
