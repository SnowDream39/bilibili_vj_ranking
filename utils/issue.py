# utils/issue.py
from pathlib import Path
import re
from datetime import datetime, timedelta
from typing import Tuple, List
import pandas as pd
from utils.logger import logger

class Issue:
    def __init__(self, total_dir: Path, newsong_dir: Path, first_issue_date: str):
        self.total_dir = total_dir
        self.newsong_dir = newsong_dir
        self.first_issue_date = first_issue_date

    def get_latest_total_excel(self) -> Path:
        files = list(self.total_dir.glob("*.xlsx"))
        latest = max(files, key=lambda p: p.stat().st_mtime)
        return latest

    def get_newsong_excel(self, total_excel_path: Path) -> Path:
        m = re.search(r"(20\d{6})", total_excel_path.stem)
        date_str = m.group(1)
        
        candidates = [p for p in self.newsong_dir.glob("*.xlsx") if date_str in p.stem]
        return candidates[0]

    def infer_issue_info(self, excel_path: Path):
        stem = excel_path.stem
        m = re.search(r"(20\d{6})", stem)
        excel_date_str = m.group(1) if m else self.first_issue_date

        excel_dt = datetime.strptime(excel_date_str, "%Y%m%d")
        first_dt = datetime.strptime(self.first_issue_date, "%Y%m%d")
        
        issue_video_dt = excel_dt - timedelta(days=1)
        issue_date_str = issue_video_dt.strftime("%Y%m%d")
        
        diff = (issue_video_dt - first_dt).days
        issue_index = max(1, diff + 1)
        
        logger.info(f"日期: {issue_date_str}, 期数: {issue_index}")
        return issue_date_str, issue_index, excel_date_str

    def prepare_video_data(self, top_n: int) -> Tuple[List[pd.Series], str, int, str]:
        excel_path = self.get_latest_total_excel()
        issue_date, idx, ex_date = self.infer_issue_info(excel_path)
        
        df_total = pd.read_excel(excel_path, dtype={'bvid': str})
        df_top = df_total.sort_values("rank").head(top_n).sort_values("rank", ascending=False)
        
        top_bvids = set()
        for _, r in df_top.iterrows():
            if pd.notna(r['bvid']):
                top_bvids.add(str(r['bvid']).strip())

        newsong_path = self.get_newsong_excel(excel_path)
        df_new = pd.read_excel(newsong_path, dtype={'bvid': str})
        if "rank" in df_new.columns:
            df_new = df_new.sort_values("rank")

        new_rows = []
        for _, row in df_new.iterrows():
            if str(row['bvid']).strip() not in top_bvids:
                new_rows.append(row)
                if len(new_rows) >= 2:
                    break
        combined = []
        for r in reversed(new_rows):
            s = r.copy()
            s['is_new'] = True
            s['rank'] = 999 
            combined.append(s)
            
        for _, r in df_top.iterrows():
            s = r.copy()
            rank_before = str(r.get('rank_before', '-')).strip()
            if rank_before == '-':
                s['is_new'] = True
            else:
                s['is_new'] = False
                
            combined.append(s)

        return combined, issue_date, idx, ex_date
