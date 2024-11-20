from pathlib import Path
from datetime import datetime
from typing import Optional, List
import pandas as pd

class RankingSummarizer:
    def __init__(self, folder_path: str, start_date: Optional[str] = None):
        self.folder_path = Path(folder_path)
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
        self.columns = ['name', 'author', 'vocal', 'point']

    def _get_valid_files(self, file_pattern: str) -> List[Path]:
        """获取符合条件的文件列表"""
        files = sorted([f for f in self.folder_path.glob("*.xlsx") if file_pattern in f.name])
        
        if self.start_date:
            filtered_files = []
            for file in files:
                if "与" in file.name:  # 日刊
                    file_date = datetime.strptime(file.name.split('与')[1].replace('.xlsx', ''), "%Y%m%d")
                else:  # 周刊
                    file_date = datetime.strptime(file.stem, "%Y-%m-%d")
                
                if file_date >= self.start_date:
                    filtered_files.append(file)
            return filtered_files
        return files

    def _process_file(self, file_path: Path, is_daily: bool) -> pd.DataFrame:
        """处理单个文件"""
        df = pd.read_excel(file_path)
        rank_1_data = df[df['rank'] == 1].copy()

        selected_columns = [col for col in self.columns if col in rank_1_data.columns]
        rank_1_data = rank_1_data[selected_columns]

        if is_daily:
            date = file_path.name.split('与')[1].replace('.xlsx', '')
        else:
            date = file_path.stem
        rank_1_data['date'] = date

        return rank_1_data

    def summarize(self, is_daily: bool = True) -> pd.DataFrame:
        """汇总排名数据"""
        pattern = "与" if is_daily else "-"
        files = self._get_valid_files(pattern)
        
        if not files:
            raise ValueError("No valid files found!")

        all_rank_1 = pd.DataFrame()
        for file in files:
            try:
                rank_1_data = self._process_file(file, is_daily)
                all_rank_1 = pd.concat([all_rank_1, rank_1_data], ignore_index=True)
                print(f"处理文件: {file.name}")
            except Exception as e:
                print(f"处理文件 {file.name} 时出错: {e}")

        return all_rank_1

    def save_summary(self, df: pd.DataFrame, output_name: str = "rank_1_summary.xlsx"):
        output_path = self.folder_path / output_name
        df.to_excel(output_path, index=False)
        print(f"排名汇总已保存至: {output_path}")

def main():
    mode = 0  # 0: 日刊, 1: 周刊
    start_date = "2024-10-01"  # 可选的开始日期，不需要时设为 None

    if mode == 0:
        folder_path = r"差异\合并表格"
    else:
        folder_path = r"周刊\总榜"

    try:
        summarizer = RankingSummarizer(folder_path, start_date)
        summary_df = summarizer.summarize(is_daily=(mode == 0))
        summarizer.save_summary(summary_df)
        
    except Exception as e:
        print(f"程序执行出错: {e}")

if __name__ == "__main__":
    main()
