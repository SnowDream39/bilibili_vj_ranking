#每日第一名汇总

import pandas as pd
import os

def extract_rank_1(folder_path, output_file_path):
    # 创建一个空的DataFrame用于存储结果
    all_rank_1 = pd.DataFrame()

    # 获取所有符合命名格式的文件
    files = sorted([f for f in os.listdir(folder_path) if f.endswith('.xlsx') and '与' in f])

    # 按时间顺序处理每个文件
    for file in files:
        file_path = os.path.join(folder_path, file)
        df = pd.read_excel(file_path)

        # 提取rank=1的行
        rank_1_data = df[df['rank'] == 1].copy()  # 创建副本

        # 只保留指定的列，如果列不存在则跳过
        selected_columns = [col for col in ['name', 'author', 'vocal', 'point'] if col in rank_1_data.columns]
        rank_1_data = rank_1_data[selected_columns]

        # 添加日期信息，提取文件名中 "与" 和 ".xlsx" 之间的日期
        rank_1_data['date'] = file.split('与')[1].replace('.xlsx', '')

        # 将结果添加到汇总的DataFrame中
        all_rank_1 = pd.concat([all_rank_1, rank_1_data], ignore_index=True)

    # 保存汇总后的数据到新表格
    all_rank_1.to_excel(output_file_path, index=False)
    print(f"Rank 1 data saved to {output_file_path}")

# 文件夹路径
folder_path = r"E:\Programming\python\bilibili日V周刊\差异\临时"

# 汇总结果保存路径
output_file_path = os.path.join(folder_path, "rank_1_summary.xlsx")

# 执行任务
extract_rank_1(folder_path, output_file_path)
