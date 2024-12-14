from dataclasses import dataclass
from typing import List, Dict, Tuple, Set
import pandas as pd
from collections import deque
from datetime import datetime, timedelta
import os
from pathlib import Path
from enum import Enum

@dataclass
class Achievement:
    """成就数据类"""
    name: str
    conditions: dict
    description: str

class AchievementType(Enum):
    EMERGING_HIT = "Emerging Hit!"
    MEGA_HIT = "Mega Hit!!!"
    POTENTIAL_REGULAR = "门番候补"
    REGULAR = "门番"

class AchievementTracker:
    """成就追踪器"""
    def __init__(self):
        self.achievements = {
            AchievementType.EMERGING_HIT: Achievement(
                name="Emerging Hit!",
                conditions={"weeks": 3, "rank": 5, "threshold": 3},
                description="连续3周位列前5"
            ),
            AchievementType.MEGA_HIT: Achievement(
                name="Mega Hit!!!",
                conditions={"weeks": 5, "rank": 3, "threshold": 5},
                description="连续5周位列前3"
            ),
            AchievementType.POTENTIAL_REGULAR: Achievement(
                name="门番候补",
                conditions={"weeks": 15, "rank": 20, "threshold": 10},
                description="15周内进入前20达10次"
            ),
            AchievementType.REGULAR: Achievement(
                name="门番",
                conditions={"weeks": 30, "rank": 20, "threshold": 20},
                description="30周内进入前20达20次"
            )
        }

    def detect_status(self, history: deque, song_name: str) -> Set[AchievementType]:
        """检测歌曲是否达成成就"""
        results = set()
        history_list = list(history)

        for achievement_type, achievement in self.achievements.items():
            conditions = achievement.conditions
            recent_weeks = history_list[-conditions["weeks"]:]
            
            if achievement_type in {AchievementType.EMERGING_HIT, AchievementType.MEGA_HIT}:
                if (len(recent_weeks) >= conditions["weeks"] and 
                    all(song_name in week[:conditions["rank"]] for week in recent_weeks)):
                    results.add(achievement_type)
            else:
                count_in_top = sum(song_name in week[:conditions["rank"]] for week in recent_weeks)
                if count_in_top >= conditions["threshold"]:
                    results.add(achievement_type)

        return results

class WeeklyHonor:
    def __init__(self, start_file: str, folder_path: str, output_folder: str, start_index: int):
        self.start_file = Path(start_file)
        self.folder_path = Path(folder_path)
        self.output_folder = Path(output_folder)
        self.start_index = start_index
        self.tracker = AchievementTracker()
        self.history = deque(maxlen=30)

    def get_weekly_files(self) -> List[Tuple[Path, datetime]]:
        start_date = datetime.strptime(self.start_file.stem, "%Y-%m-%d")
        files = sorted(self.folder_path.glob("*.xlsx"))
        
        valid_files = []
        current_date = start_date
        while True:
            current_file = self.folder_path / f"{current_date.strftime('%Y-%m-%d')}.xlsx"
            if current_file in files:
                valid_files.append((current_file, current_date))
                current_date += timedelta(days=7)
            else:
                break
        return valid_files

    def weekly_processing(self, file_path: Path) -> List[str]:
        df = pd.read_excel(file_path)
        return df['name'][:20].tolist()

    def save_achievements(self, achievements: Dict[AchievementType, List[List]]):
        """保存成就数据"""
        self.output_folder.mkdir(parents=True, exist_ok=True)
        
        with pd.ExcelWriter(self.output_folder / '成就.xlsx', engine='openpyxl') as writer:
            for achievement_type, songs in achievements.items():
                if songs:
                    df = pd.DataFrame(songs, columns=['name', 'index'])
                    df.to_excel(writer, sheet_name=achievement_type.value, index=False)

    def main_processing(self):
        weekly_files = self.get_weekly_files()
        achievements = {achievement_type: [] for achievement_type in AchievementType}

        for file_index, (file_path, _) in enumerate(weekly_files, self.start_index):
            try:
                top_20 = self.weekly_processing(file_path)
                self.history.append(top_20)

                for song_name in top_20:
                    achieved = self.tracker.detect_status(self.history, song_name)
                    for achievement_type in achieved:
                        if not any(entry[0] == song_name for entry in achievements[achievement_type]):
                            achievements[achievement_type].append([song_name, file_index])

                print(f"第 {file_index} 期处理完成")
            except Exception as e:
                print(f"处理第 {file_index} 期时出错: {e}")

        self.save_achievements(achievements)

def main():
    processor = WeeklyHonor(
        start_file="周刊/总榜/2024-09-07.xlsx",
        folder_path="周刊/总榜",
        output_folder="成就/周刊",
        start_index=1
    )
    processor.main_processing()

if __name__ == "__main__":
    main()
