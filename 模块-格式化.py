#格式化
import pandas as pd
import os
from datetime import datetime, timedelta

# 文件夹路径
source_folder = "差异/合并表格/"
destination_folder = "测试内容/合并表格/"
main_tracklist_path = "收录曲目.xlsx"
def merge_duplicate_names(df):
    """对name同名但bvid不同的记录，根据point选择分数最高的一个，并继承count等数据"""
    return df.loc[df.groupby('name')['point'].idxmax()]
# 创建输出文件夹（如果不存在）
if not os.path.exists(destination_folder):
    os.makedirs(destination_folder)

# 读取总的收录曲目表格
main_df = pd.read_excel(main_tracklist_path)

# 设置起始日期
today = datetime(2024, 7, 3)  # 从2024年7月2日开始处理

# 遍历源文件夹中的所有文件
for file_name in sorted(os.listdir(source_folder)):
    if file_name.endswith(".xlsx"):
        # 获取当前文件日期
        current_date_str = file_name.split("与")[0]
        current_date = datetime.strptime(current_date_str, "%Y%m%d")

        # 检查当前文件是否在今天的范围内
        if current_date < today:
            continue

        print(f"处理文件: {file_name}")

        # 读取旧数据文件
        old_df = pd.read_excel(os.path.join(source_folder, file_name))

        # 删除可能存在的相关列
        columns_to_remove = ['vocal', 'synthesizer', 'rank', 'image_url', 'type']
        existing_columns = [col for col in columns_to_remove if col in old_df.columns]
        old_df.drop(columns=existing_columns, inplace=True)

        # 合并数据
        merged_df = old_df.merge(
            main_df[['Title', 'Vocal', 'Synthesizer', 'Type', 'image_url']],
            left_on='name',
            right_on='Title',
            how='left'
        )

        # 重命名和填充新列
        merged_df.rename(columns={
            'Vocal': 'vocal',
            'Synthesizer': 'synthesizer',
            'Type': 'type',
            'image_url': 'image_url'
        }, inplace=True)

        # 计算当前排名
        merged_df['rank'] = merged_df['point'].rank(method='max', ascending=False)

        # 使用 merge_duplicate_names 函数处理重复的 name
        merged_df = merge_duplicate_names(merged_df)

        # 处理上一天的数据
        if current_date > datetime(2024, 7, 2):
            previous_date = current_date - timedelta(days=1)
            previous_file_name = f"{previous_date.strftime('%Y%m%d')}与{(previous_date - timedelta(days=1)).strftime('%Y%m%d')}.xlsx"
            previous_day_file_path = os.path.join(destination_folder, previous_file_name)

            # 检查上一期文件是否存在
            if not os.path.exists(previous_day_file_path):
                print(f"未找到文件: {previous_file_name}, 跳过处理 {file_name}。")
                continue

            # 读取上一天的数据
            previous_df = pd.read_excel(previous_day_file_path)

            # 检查 last_data 是否有数据，并处理重复的 name
            if 'name' in previous_df.columns and 'rank' in previous_df.columns and 'point' in previous_df.columns:
                # 使用 merge_duplicate_names 函数处理重复的 name
                last_data = previous_df[['name', 'rank', 'point']]
                last_data = merge_duplicate_names(last_data)  # 处理重复的 name

                # 将 last_data 转换为字典以便更好的映射
                last_data_dict = last_data.set_index('name').to_dict(orient='index')

                # 填充 rank_before 和 point_before
                merged_df['rank_before'] = merged_df['name'].map(lambda x: last_data_dict[x]['rank'] if x in last_data_dict else '-')
                merged_df['point_before'] = merged_df['name'].map(lambda x: last_data_dict[x]['point'] if x in last_data_dict else '-')

                # 计算 rate
                merged_df['rate'] = merged_df.apply(
                    lambda row: 'NEW' 
                    if row['point_before'] == '-' else
                    'inf' if row['point_before'] == 0 else
                    f"{(row['point'] - row['point_before']) / row['point_before']:.2%}",
                    axis=1
                )

        # 格式化 viewR, favoriteR, coinR, likeR
        for col in ['viewR', 'favoriteR', 'coinR', 'likeR']:
            if col in merged_df.columns:
                merged_df[col] = merged_df[col].map(lambda x: f"{x:.2f}" if pd.notna(x) else '-')

        # 根据 point 排序并选择列
        final_df = merged_df.reindex(columns=['title', 'bvid', 'name', 'author', 'uploader', 'copyright', 
                                               'synthesizer', 'vocal', 'type', 'pubdate', 'duration', 
                                               'page', 'view', 'favorite', 'coin', 'like', 'viewR', 
                                               'favoriteR', 'coinR', 'likeR', 'point', 'image_url', 
                                               'view_rank', 'favorite_rank', 'coin_rank', 'like_rank', 
                                               'rank', 'rank_before', 'point_before', 'rate', 'count'])
        final_df.sort_values(by='point', ascending=False, inplace=True)

        # 保存处理后的文件
        destination_file_path = os.path.join(destination_folder, file_name)
        final_df.to_excel(destination_file_path, index=False)

        # 更新 today 为下一个日期
        today += timedelta(days=1)

print("文件夹中所有文件已处理并保存到目标文件夹。")
