from datetime import datetime, timedelta
import pandas as pd

# modes = ['日', '周', '月']
mode = 1
new_date = "20241116"

if mode == 0:
    num_new = 10
    old_date = (datetime.strptime(new_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
    infile_toll = f'差异/合并表格/{new_date}与{old_date}.xlsx'
    infile_new = f'新曲榜/新曲榜{new_date}与{old_date}.xlsx'
    outfile = f'抄榜/日/{new_date}与{old_date}.txt'
elif mode == 1:
    num_new = 10
    new_date_file = f'{new_date[:4]}-{new_date[4:6]}-{new_date[6:]}'
    old_date = (datetime.strptime(new_date, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
    old_date_file = f'{old_date[:4]}-{old_date[4:6]}-{old_date[6:]}'
    infile_toll = f'周刊/总榜/{new_date_file}.xlsx'
    infile_new = f'周刊/新曲榜/新曲{new_date_file}.xlsx'
    outfile = f'抄榜/周/{new_date_file}.txt'
elif mode == 2:
    num_new = 20
    new_date_file = f'{new_date[:4]}-{new_date[4:6]}'
    old_date = (datetime.strptime(new_date, "%Y%m%d") - timedelta(days=30)).strftime("%Y%m%d")
    old_date_file = f'{old_date[:4]}-{old_date[4:6]}'
    infile_toll = f'月刊/总榜/{old_date_file}.xlsx'
    infile_new = f'月刊/新曲榜/新曲{old_date_file}.xlsx'
    outfile = f'抄榜/月/{old_date_file}.txt'


df_toll = pd.read_excel(infile_toll)
df_new = pd.read_excel(infile_new)

df_new_top = df_new.sort_values(by='rank', ascending=True).head(num_new).sort_values(by='rank', ascending=False)
df_toll_top = df_toll.sort_values(by='rank', ascending=True).head(20).sort_values(by='rank', ascending=False)

new_output_lines = df_new_top.apply(lambda row: f"{row['rank']}.【{row['vocal']}{' Cover' if row['type'] == '翻唱' else ''}】{row['name']}【{row['author']}】 {row['bvid']}", axis=1)
toll_output_lines = df_toll_top.apply(lambda row: f"{row['rank']}.【{row['vocal']}{' Cover' if row['type'] == '翻唱' else ''}】{row['name']}【{row['author']}】 {row['bvid']}", axis=1)

with open(outfile, 'w', encoding='utf-8') as f:
    f.write(f"新曲榜\n")
    for line in new_output_lines:
        f.write(line + '\n')
    f.write(f"\n\n总榜\n")
    for line in toll_output_lines:
        f.write(line + '\n')

print(f"结果已写入：{outfile}")
