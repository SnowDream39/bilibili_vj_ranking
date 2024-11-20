#初始化count值

import pandas as pd
import os

def update_count(file_path, prev_file_path):
    # 读取今天和昨天的文件
    df_today = pd.read_excel(file_path)
    
    if os.path.exists(prev_file_path):
        df_prev = pd.read_excel(prev_file_path)
        if 'count' not in df_prev.columns:
            df_prev['count'] = 0  # 如果没有count列，初始化为0
    else:
        df_prev = pd.DataFrame(columns=['name', 'count'])

    # 创建字典以加快查找上一天的count值
    prev_count_dict = dict(zip(df_prev['name'], df_prev['count']))

    # 初始化今天的count列
    df_today['count'] = 0  # 默认值为0

    # 更新count值
    for i, row in df_today.iterrows():
        name = row['name']
        rank = row['rank']

        if name in prev_count_dict:
            if rank <= 20:
                df_today.at[i, 'count'] = prev_count_dict[name] + 1
            else:
                df_today.at[i, 'count'] = prev_count_dict[name]
        else:
            if rank <= 20:
                df_today.at[i, 'count'] = 1  
            else:
                df_today.at[i, 'count'] = 0  

    # 直接覆盖保存更新后的文件
    df_today.to_excel(file_path, index=False)
    print(f"Updated file saved to {file_path}")

# 文件夹路径
folder_path = r"E:\Programming\python\bilibili日V周刊\测试内容\合并表格"

# 获取所有符合命名格式的文件
files = sorted([f for f in os.listdir(folder_path) if f.endswith('.xlsx') and '与' in f])

# 从第二个文件开始处理
for i in range(1, len(files)):
    current_file = os.path.join(folder_path, files[i])
    previous_file = os.path.join(folder_path, files[i-1])
    update_count(current_file, previous_file)