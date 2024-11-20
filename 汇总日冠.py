#每日第一名汇总
import pandas as pd
import os

mode = 0  #0 :日刊 1 :周刊
def daily_rank(folder_path, output_file_path):
    all_rank_1 = pd.DataFrame()
    files = sorted([f for f in os.listdir(folder_path) if f.endswith('.xlsx') and '与' in f])
    for file in files:
        file_path = os.path.join(folder_path, file)
        df = pd.read_excel(file_path)

        rank_1_data = df[df['rank'] == 1].copy()  

        selected_columns = [col for col in ['name', 'author', 'vocal', 'point'] if col in rank_1_data.columns]
        rank_1_data = rank_1_data[selected_columns]
        rank_1_data['date'] = file.split('与')[1].replace('.xlsx', '')
        all_rank_1 = pd.concat([all_rank_1, rank_1_data], ignore_index=True)

    all_rank_1.to_excel(output_file_path, index=False)
    print(f"Rank 1 data saved to {output_file_path}")

def weekly_rank(folder_path, output_file_path):
    all_rank_1 = pd.DataFrame()
    files = sorted([f for f in os.listdir(folder_path) if f.endswith('.xlsx') and '-' in f])
    for file in files:
        file_path = os.path.join(folder_path, file)
        df = pd.read_excel(file_path)

        rank_1_data = df[df['rank'] == 1].copy()  

        selected_columns = [col for col in ['name', 'author', 'vocal', 'point'] if col in rank_1_data.columns]
        rank_1_data = rank_1_data[selected_columns]
        rank_1_data['date'] = file.replace('.xlsx', '')
        all_rank_1 = pd.concat([all_rank_1, rank_1_data], ignore_index=True)
     
    all_rank_1.to_excel(output_file_path, index=False)
    print(f"Rank 1 data saved to {output_file_path}")
if mode == 0:
    folder_path = r"E:\Programming\python\bilibili日V周刊\差异\合并表格"
    output_file_path = os.path.join(folder_path, "rank_1_summary.xlsx")
    daily_rank(folder_path, output_file_path)

else:
    folder_path = r"E:\Programming\python\bilibili日V周刊\周刊\总榜"
    output_file_path = os.path.join(folder_path, "rank_1_summary.xlsx")
    weekly_rank(folder_path, output_file_path)