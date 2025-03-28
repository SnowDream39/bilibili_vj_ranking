import pandas as pd
from src.processing import process_records
from utils.calculator import calculate_ranks
from utils.io_utils import save_to_excel

song_data = '梦的结唱4自动标注'

CONFIG = {
    "paths": {
        "input_path": f"特殊/特殊原始数据/{song_data}.xlsx",
        "output_path": f"特殊/特殊排行榜/{song_data}.xlsx"
    }
}

def main_processing(input_path, output_path):
    df = pd.read_excel(input_path)
    df = process_records(
        new_data = df,
        use_old_data = False,
        use_collected = True,
        ranking_type='special',
        collected_data = pd.read_excel('收录曲目.xlsx')
    )
    df = df.loc[df.groupby('name')['point'].idxmax()].reset_index(drop=True)  
    df = calculate_ranks(df)
    save_to_excel(df, output_path)

if __name__ == "__main__":
    main_processing(CONFIG["paths"]["input_path"],CONFIG["paths"]["output_path"])
