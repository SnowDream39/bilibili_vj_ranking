#用于生成最高最低数据演替

import os
import pandas as pd

def get_files_in_directory(directory, start_file):
    """Get a list of files in the directory sorted by date in the filename starting from start_file."""
    files = [f for f in os.listdir(directory) if f.endswith('.xlsx') and '与' in f]
    files.sort()  

    start_index = files.index(start_file)
    return files[start_index:]

def extract_date_from_filename(filename):
    """Extract the date part from the filename."""
    return filename.split('与')[1].split('.')[0]

def insert_empty_columns(df):
    """Insert empty columns between days."""
    cols = df.columns
    new_cols = []
    for i, col in enumerate(cols):
        new_cols.append(col)
        if (i + 1) % 3 == 0:  
            new_cols.append('')
    new_df = pd.DataFrame(columns=new_cols)

    for i in range(len(df)):
        row = []
        for j in range(len(df.columns)):
            row.append(df.iloc[i, j])
            if (j + 1) % 3 == 0:
                row.append(None)
        new_df.loc[i] = row

    return new_df

def adjust_column_widths(writer, sheet_names):
    """Adjust the column widths to fit the content for all specified sheet names."""
    for sheet_name in sheet_names:
        worksheet = writer.sheets[sheet_name]
        for col in worksheet.columns:
            max_length = 0
            column = col[0].column_letter  
            for cell in col:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = (max_length + 2)
            worksheet.column_dimensions[column].width = adjusted_width

def process_files(directory, start_file, metrics):
    """Process the files to find the historical highest and lowest values for the top 20 ranks for specified metrics."""
    files = get_files_in_directory(directory, start_file)

    highest_records = {metric: {rank: -float('inf') for rank in range(1, 21)} for metric in metrics}
    lowest_records = {metric: {rank: float('inf') for rank in range(1, 21)} for metric in metrics}
    
    highest_data = {metric: {rank: [] for rank in range(1, 21)} for metric in metrics}
    lowest_data = {metric: {rank: [] for rank in range(1, 21)} for metric in metrics}

    for file in files:
        filepath = os.path.join(directory, file)
        df = pd.read_excel(filepath)

        if not df.empty:
            for metric in metrics:
                if metric in ['point', 'rank']:
                    metric_col = 'rank'
                    value_col = metric
                else:
                    metric_col = f'{metric}_rank'
                    value_col = metric

                for rank in range(1, 21):
                    if metric_col in df.columns and rank in df[metric_col].values:
                        row = df[df[metric_col] == rank]
                        value = row[value_col].values[0]
                        name = row['name'].values[0]

                        if value >= highest_records[metric][rank]:
                            highest_records[metric][rank] = value
                            highest_data[metric][rank].append((extract_date_from_filename(file), value, name))

                        if value <= lowest_records[metric][rank]:
                            lowest_records[metric][rank] = value
                            lowest_data[metric][rank].append((extract_date_from_filename(file), value, name))

    def prepare_data(records):
        """Prepare data for DataFrame."""
        data = {}
        max_length = max(len(records[rank]) for rank in range(1, 21))
        for rank in range(1, 21):
            dates = [record[0] for record in records[rank]]
            values = [record[1] for record in records[rank]]
            names = [record[2] for record in records[rank]]

            dates += [None] * (max_length - len(dates))
            values += [None] * (max_length - len(values))
            names += [None] * (max_length - len(names))

            data[f'Rank{rank} Date'] = dates
            data[f'Rank{rank} Value'] = values
            data[f'Rank{rank} Name'] = names

        df = pd.DataFrame(data)
        return insert_empty_columns(df)

    dfs = {}
    for metric in metrics:
        highest_df = prepare_data(highest_data[metric])
        lowest_df = prepare_data(lowest_data[metric])
        dfs[f'{metric.capitalize()} Highest'] = highest_df
        dfs[f'{metric.capitalize()} Lowest'] = lowest_df

    output_path = os.path.join(directory, 'top_20_rank_summary.xlsx')
    with pd.ExcelWriter(output_path) as writer:
        for sheet_name, df in dfs.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        adjust_column_widths(writer, dfs.keys())

    print(f"Top 20 ranks summary saved to {output_path}")

directory = r'E:\Programming\python\bilibili日V周刊\差异\合并表格'
start_file = '20240704与20240703.xlsx'
metrics = ['point', 'view', 'favorite', 'coin', 'like']

process_files(directory, start_file, metrics)
