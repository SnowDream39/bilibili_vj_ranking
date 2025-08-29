from dataclasses import dataclass
from typing import List, Dict, Tuple, Set, Optional, Union
import pandas as pd
from collections import deque, defaultdict
from datetime import datetime
from pathlib import Path
from enum import Enum
from utils.logger import logger

@dataclass
class AchiDef:
    name: str
    condition: Dict[str, int]

class AchiType(Enum):
    EMERGING_HIT = "Emerging Hit!"
    MEGA_HIT = "Mega Hit!!!"
    POTENTIAL_REGULAR = "门番候补"
    REGULAR = "门番"

@dataclass
class AchievedSong:
    name: str
    index: int
    honor: str
    title: Optional[str] = None
    bvid: Optional[str] = None
    author: Optional[str] = None
    pubdate: Optional[str] = None

class Achievement:
    def __init__(self):
        self.definition: Dict[AchiType, AchiDef] = {
            AchiType.EMERGING_HIT: AchiDef(
                name="Emerging Hit!",
                condition={"weeks": 3, "rank": 5}
            ),
            AchiType.MEGA_HIT: AchiDef(
                name="Mega Hit!!!",
                condition={"weeks": 5, "rank": 3}
            ),
            AchiType.POTENTIAL_REGULAR: AchiDef(
                name="门番候补",
                condition={"weeks": 15, "rank": 20, "threshold": 10}
            ),
            AchiType.REGULAR: AchiDef(
                name="门番",
                condition={"weeks": 30, "rank": 20, "threshold": 20}
            )
        }
        self.window_size = max(d.condition.get("weeks", 1) for d in self.definition.values()) if self.definition else 0

    def detect(self, history: deque[List[str]], name: str) -> Set[AchiType]: 
        achieved: Set[AchiType] = set()
        hist_list = list(history)

        for type, definition in self.definition.items():
            condition = definition.condition
            weeks = condition["weeks"]
            hist_slice = hist_list[-weeks:]

            if type in {AchiType.EMERGING_HIT, AchiType.MEGA_HIT}:
                if len(hist_slice) == weeks and \
                    all(name in week[:condition["rank"]] for week in hist_slice if week):
                    achieved.add(type)
            else:
                count = sum(name in week[:condition["rank"]] for week in hist_slice if week)
                if count >= condition["threshold"]:
                    achieved.add(type)
        return achieved

class WeeklyHonor:
    def __init__(self, output_dir: str, start_period: int, period_data: Dict[int, Tuple[List[str], pd.DataFrame, str]], target_period: int, window_size: int):

        self.output_path = Path(output_dir)
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.start_period = start_period
        self.target_period = target_period
        self.period_data = period_data
 
        self.tracker = Achievement()
        self.history: deque[List[str]] = deque(maxlen=window_size)
        self.master_file = self.output_path / "成就.xlsx"
        self.master_db: Dict[AchiType, Dict[str, Tuple[int, str]]] = self.load_master_db()

    def load_master_db(self) -> Dict[AchiType, Dict[str, Tuple[int, str]]]:
        db: Dict[AchiType, Dict[str, Tuple[int, str]]] = defaultdict(dict)
        if not self.master_file.exists():
            return db
        
        file = pd.ExcelFile(self.master_file, engine = 'openpyxl')
        for type in AchiType:
            sheet = type.value
            if sheet in file.sheet_names:
                df = pd.read_excel(file, sheet_name = sheet)
                for _, row in df.iterrows():
                    db[type][str(row['name'])] = (int(row['index']), str(row.get('progress', '')))
        return db

    def _save_master_db(self):
        with pd.ExcelWriter(self.master_file, engine='openpyxl') as writer:
            for type, song_map in self.master_db.items():
                if song_map:
                    rows: List[Dict[str, Union[str, int]]] = [{'name': name, 'index': idx, 'progress': progress}
                                for name, (idx, progress) in sorted(song_map.items(), key=lambda item: (item[1][0], item[0]))]
                    df = pd.DataFrame(rows)
                    df.to_excel(writer, sheet_name = type.value, index=False)

    def save_report(self, date: str, report_data: Dict[AchiType, List[AchievedSong]]):
        output_file = self.output_path / f"成就{date}.xlsx"
        usecols = ['title', 'bvid', 'name', 'author', 'pubdate', 'honor']
        all_dfs: List[pd.DataFrame] = []
        for type in AchiType:
            songs = report_data.get(type, [])
            rows: List[Dict[str, str]] = []
            if songs:
                songs = sorted(songs, key=lambda song: song.name)
                for song in songs:
                    rows.append({
                       'title': song.title or '', 'bvid': song.bvid or '',
                       'name': song.name, 'author': song.author or '',
                       'pubdate': song.pubdate or '', 'honor': song.honor
                    })
            df = pd.DataFrame(rows, columns = usecols)
            
            if not df.empty:
                all_dfs.append(df)
            
        if not all_dfs: 
            logger.info(f"当期{date}没有成就")
            final_df = pd.DataFrame(columns = usecols)
        else:
            final_df = pd.concat(all_dfs, ignore_index=True)
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            final_df.to_excel(writer, index=False)
            
        logger.info(f"已保存成就报告: {output_file.name}")

    @staticmethod
    def sort_files(path: Path) -> List[Tuple[Path, datetime]]:
        files: List[Tuple[Path, datetime]] = []
        for file_path in sorted(path.glob("*.xlsx")):
            date_str = file_path.stem.split(" ")[-1] if " " in file_path.stem else file_path.stem
            date = datetime.strptime(date_str, "%Y-%m-%d")
            files.append((file_path, date))
        files.sort(key=lambda x: (x[1], x[0].stem))
        return files

    @staticmethod
    def process_file(file_path: Path) -> Tuple[List[str], pd.DataFrame]:
        df = pd.read_excel(file_path, engine='openpyxl')
        reqcols = ['name', 'title', 'bvid', 'author', 'pubdate']
        top = df.head(20).copy()
        names: List[str] = []
        for index in top.index:
            name = top.loc[index, 'name']
            name = str(name).strip() if pd.notna(name) else ""
            names.append(name)
            top.loc[index, 'name'] = name

        details = top[reqcols]
        return names, details

    def _calculate_hit_progress(self, hist_slice: List[List[str]], name: str, condition: int) -> str:
        ranks = [str(week.index(name) + 1) if name in week[:condition] else ("X" if name in week else "-")
                 for week in hist_slice]
        return "~".join(ranks)

    def _calculate_regular_progress(self, hist_slice: List[List[str]], name: str, condition: int) -> str:
        count = sum(name in week[:condition] for week in hist_slice if week)
        first_index = next((i for i, week in enumerate(hist_slice) if week and name in week[:condition]), -1)
        elapsed_weeks = len(hist_slice) - first_index if first_index != -1 else len(hist_slice)
        return f"{count}/{elapsed_weeks}"
    
    def run(self):
        report_data: Dict[AchiType, List[AchievedSong]] = defaultdict(list)

        for period in range(self.start_period, self.target_period + 1):
            names, details, date_str = self.period_data[period]
            self.history.append(names)

            if period < self.target_period:
                continue

            unique_songs = set(name for week in self.history for name in week if name)
            for name in unique_songs:
                if not name: continue
                
                achieved_types = self.tracker.detect(self.history, name)
                for type in achieved_types:
                    if name not in self.master_db[type]:
                        definition = self.tracker.definition[type]
                        condition = definition.condition
                        hist_list = list(self.history)
                        weeks, rank = condition["weeks"], condition["rank"]
                        hist_slice = hist_list[-weeks:]
                        progress = ""

                        if type in {AchiType.EMERGING_HIT, AchiType.MEGA_HIT}:
                            progress = self._calculate_hit_progress(hist_slice, name, rank)
                        elif type in {AchiType.POTENTIAL_REGULAR, AchiType.REGULAR}:
                            progress = self._calculate_regular_progress(hist_slice, name, rank)

                        title, bvid, author, pubdate = None, None, None, None
                        if not details.empty:
                            row = details[details['name'] == name]
                            if not row.empty:
                                data = row.iloc[0]
                                title = str(data.get('title', ''))
                                bvid = str(data.get('bvid', ''))
                                author = str(data.get('author', ''))
                                pubdate = data.get('pubdate')
                                pubdate: Optional[str] = pd.to_datetime(pubdate).strftime('%Y-%m-%d %H:%M:%S') if pd.notna(pubdate) else str(pubdate)

                        record = AchievedSong(name, period, type.value, title, bvid, author, pubdate)
                        report_data[type].append(record)
                        self.master_db[type][name] = (period, progress)
                        logger.info(f"新成就: '{name}', 期{period}, {type.value}, {progress}")

            self.save_report(date_str, report_data)

        self._save_master_db()

def main():
    START_IDX = 1
    START_FILE = "2024-09-07.xlsx"
    IN_DIR = "周刊/总榜"
    OUT_DIR = "成就/"

    config = Achievement()
    window_size = config.window_size 

    input_path = Path(IN_DIR)
    output_path = Path(OUT_DIR)
    output_path.mkdir(parents=True, exist_ok=True)

    all_files = WeeklyHonor.sort_files(input_path)
    start_file_stem = Path(START_FILE).stem.split(" ")[-1] if " " in Path(START_FILE).stem else Path(START_FILE).stem

    start_index = next(i for i, (_, dt) in enumerate(all_files)
                                if dt.strftime("%Y-%m-%d") == start_file_stem)
    latest_period = START_IDX + (len(all_files) - 1 - start_index)
    target_period = latest_period
    start_period = max(START_IDX, target_period - window_size + 1)
    end_period = target_period

    logger.info(f"目标期数: {target_period}")

    period_data: Dict[int, Tuple[List[str], pd.DataFrame, str]] = {}
    for period_to_load in range(start_period, end_period + 1):
        file_index = start_index + (period_to_load - START_IDX)
        file_path, date = all_files[file_index]
        date_str = date.strftime("%Y-%m-%d")
        names, details = WeeklyHonor.process_file(file_path)
        period_data[period_to_load] = (names, details, date_str)

    processor = WeeklyHonor(
        output_dir=OUT_DIR,
        start_period=start_period,
        period_data=period_data,
        target_period=target_period,
        window_size=window_size
    )
    processor.run()
    input()

if __name__ == "__main__":
    main()
