from dataclasses import dataclass
from typing import List, Dict, Tuple, Set, Optional
import pandas as pd
from collections import deque, defaultdict
from datetime import datetime
from pathlib import Path
from enum import Enum
from utils.logger import logger

@dataclass
class AchievementDefinition: 
    name: str 
    conditions: dict
    description: str

class AchievementType(Enum):
    EMERGING_HIT = "Emerging Hit!"
    MEGA_HIT = "Mega Hit!!!"
    POTENTIAL_REGULAR = "门番候补"
    REGULAR = "门番"

    @classmethod
    def from_value(cls, value: str) -> Optional['AchievementType']:
        for item in cls:
            if item.value == value:
                return item
        return None

@dataclass
class AchievedSong:
    name: str             
    index: int               
    honor: str   
    title: Optional[str] = None 
    bvid: Optional[str] = None
    author: Optional[str] = None
    pubdate: Optional[str] = None   

class AchievementTracker:
    def __init__(self):
        self.achievements_def: Dict[AchievementType, AchievementDefinition] = {
            AchievementType.EMERGING_HIT: AchievementDefinition(
                name="Emerging Hit!",
                conditions={"weeks": 3, "rank": 5},
                description="连续3周位列前5"
            ),
            AchievementType.MEGA_HIT: AchievementDefinition(
                name="Mega Hit!!!",
                conditions={"weeks": 5, "rank": 3},
                description="连续5周位列前3"
            ),
            AchievementType.POTENTIAL_REGULAR: AchievementDefinition(
                name="门番候补",
                conditions={"weeks": 15, "rank": 20, "threshold": 10},
                description="15周内进入前20达10次"
            ),
            AchievementType.REGULAR: AchievementDefinition(
                name="门番",
                conditions={"weeks": 30, "rank": 20, "threshold": 20},
                description="30周内进入前20达20次"
            )
        }

    def detect_status(self, history: deque, song_name: str) -> Set[AchievementType]:
        results = set()
        history_list = list(history)

        for ach_type, ach_def in self.achievements_def.items():
            conditions = ach_def.conditions
            relevant_history_slice = history_list[-conditions["weeks"]:]

            if ach_type in {AchievementType.EMERGING_HIT, AchievementType.MEGA_HIT}:
                if len(relevant_history_slice) == conditions["weeks"] and \
                   all(song_name in week_data[:conditions["rank"]] for week_data in relevant_history_slice if week_data):
                    results.add(ach_type)
            else: 
                count_in_top = sum(song_name in week_data[:conditions["rank"]] for week_data in relevant_history_slice if week_data)
                if count_in_top >= conditions["threshold"]:
                    results.add(ach_type)
        return results

class WeeklyHonor: 
    def __init__(self, start_file: str, folder_path: str, output_folder: str, start_index: int, 
                 preloaded_data: Dict[int, Tuple[List[str], pd.DataFrame]], 
                 target_end_index: Optional[int] = None):
        self.start_file_ref_name = Path(start_file).stem
        self.folder_path = Path(folder_path) 
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(parents=True, exist_ok=True)
        
        self.start_index_ref = start_index
        self.tracker = AchievementTracker()
        self.history: deque[List[str]] = deque(maxlen=30)

        self.master_achievement_file = self.output_folder / "成就.xlsx" 
        self.toll_honor: Dict[AchievementType, Dict[str, Tuple[int, str]]] = self._load_master_achievements()
        self.target_end_index = target_end_index
        self.preloaded_data = preloaded_data

    def _load_master_achievements(self) -> Dict[AchievementType, Dict[str, Tuple[int, str]]]:
        loaded_achievements = defaultdict(dict)
        if not self.master_achievement_file.exists():
            logger.info(f"主成就文件 {self.master_achievement_file} 不存在。")
            return loaded_achievements
        try:
            xls = pd.ExcelFile(self.master_achievement_file, engine='openpyxl')
            for ach_type in AchievementType:
                sheet_name = ach_type.value
                if sheet_name in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=sheet_name)
                    for _, row in df.iterrows():
                        song_name = str(row['name'])
                        achieved_index = int(row['index'])
                        progress_str = str(row.get('progress', '')) 
                        loaded_achievements[ach_type][song_name] = (achieved_index, progress_str)
        except Exception as e:
            logger.error(f"加载主成就文件 {self.master_achievement_file} 失败: {e}。")
            return defaultdict(dict) 
        return loaded_achievements

    def _save_master_achievements(self):
        try:
            with pd.ExcelWriter(self.master_achievement_file, engine='openpyxl') as writer:
                for ach_type, songs_map in self.toll_honor.items():
                    if songs_map:
                        df_data = []
                        for name, (idx, progress) in songs_map.items():
                            df_data.append({'name': name, 'index': idx, 'progress': progress})
                        
                        df = pd.DataFrame(df_data).sort_values(by=['index', 'name'])
                        df.to_excel(writer, sheet_name=ach_type.value, index=False)
        except Exception as e:
            logger.error(f"保存主成就文件 {self.master_achievement_file} 失败: {e}")

    def save_honor(self, week_idx: int, achieved_songs_map: Dict[AchievementType, List[AchievedSong]]):
        if not any(achieved_songs_map.values()):
            return

        output_file = self.output_folder / f"{week_idx}.xlsx"
        
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                for ach_type, song_obj_list in achieved_songs_map.items():
                    if song_obj_list:
                        df_data = []
                        for s_obj in song_obj_list:
                            df_data.append({
                                'title': s_obj.title or '',
                                'bvid': s_obj.bvid or '',
                                'name': s_obj.name,              
                                'author': s_obj.author or '',
                                'pubdate': s_obj.pubdate or '',
                                'honor': s_obj.honor
                            })
                        df = pd.DataFrame(df_data).sort_values(by=['name'])
                        df.to_excel(writer, sheet_name=ach_type.value, index=False) 
        except Exception as e:
            logger.error(f"保存第 {week_idx} 期成就文件 {output_file} 失败: {e}")

    @staticmethod
    def sort_file(folder_path_obj: Path) -> List[Tuple[Path, datetime]]:
        files_with_dates = []
        for file_path in sorted(folder_path_obj.glob("*.xlsx")):
            date = datetime.strptime(file_path.stem, "%Y-%m-%d")
            files_with_dates.append((file_path, date))
        return files_with_dates

    @staticmethod
    def weekly_processing(file_path: Path) -> Tuple[List[str], pd.DataFrame]:
        default_return = [], pd.DataFrame()
        try:
            df = pd.read_excel(file_path, engine='openpyxl')
            required_cols = ['name', 'title', 'bvid', 'author', 'pubdate']
            if not all(col in df.columns for col in required_cols):
                logger.error(f"文件 {file_path.name} 缺少必需列 ({', '.join(required_cols)})。")
                return default_return

            top_df = df.head(20).copy()
            
            names = []
            processed_indices = []
            for idx, name_val in top_df['name'].items(): 
                name_processed = str(name_val).strip() if pd.notna(name_val) else ""
                names.append(name_processed)
                top_df.loc[idx, 'name'] = name_processed 
                processed_indices.append(idx) 

            details_df = top_df.loc[processed_indices, required_cols]
            return names, details_df

        except Exception as e:
            logger.error(f"读取或处理文件 {file_path.name} 失败: {e}")
            return default_return

    def main_processing(self):
        max_available_preloaded_index = 0
        if self.preloaded_data:
            max_available_preloaded_index = max(self.preloaded_data.keys())


        latest_available_period_index = max_available_preloaded_index 
        determined_processing_end_idx: int
        if self.target_end_index is None:
            determined_processing_end_idx = latest_available_period_index
        else:
            if self.target_end_index < self.start_index_ref:
                logger.error(f"指定的结束期数 {self.target_end_index} 小于起始参考期数 {self.start_index_ref}。处理中止。")
                return
            
            determined_processing_end_idx = min(self.target_end_index, latest_available_period_index)

        if determined_processing_end_idx < self.start_index_ref:
            return
        
        proc_start_idx = max(self.start_index_ref, determined_processing_end_idx - self.history.maxlen + 1)
        logger.info(f"将从第 {proc_start_idx} 期开始填充历史并检测成就，直至第 {determined_processing_end_idx} 期。")

        new_ach_count = 0
        new_df = pd.DataFrame() 

        for index in range(self.start_index_ref, determined_processing_end_idx + 1): 
            weekly_data_tuple = self.preloaded_data.get(index)
            
            if weekly_data_tuple is None:
                logger.warning(f"第 {index} 期数据未在预加载数据中找到。历史记录可能不完整。")
                song_names_this_week, new_df = [], pd.DataFrame()
                if index >= proc_start_idx: 
                    self.history.append([]) 
                continue 
            
            song_names_this_week, new_df = weekly_data_tuple
            self.history.append(song_names_this_week)

            if index < proc_start_idx:
                logger.debug(f"第 {index} 期早于有效处理窗口 ({proc_start_idx})，仅用于填充历史。")
                continue
            
            logger.debug(f"处理第 {index} 期 (数据已预加载)")
            if not any(s for s in song_names_this_week if s) : 
                logger.warning(f"第 {index} 期未能提取到有效的榜单歌曲名 (从预加载数据)。")

            new_honor: Dict[AchievementType, List[AchievedSong]] = defaultdict(list) 
            
            songs_in_history = set(s_name for week_list in self.history for s_name in week_list if s_name)

            if not songs_in_history:
                logger.info(f"第 {index} 期历史记录中无有效歌曲可供检测成就。")
            
            for name in songs_in_history: 
                ach_types_for_song = self.tracker.detect_status(self.history, name)
                
                for ach_type in ach_types_for_song:
                    if name not in self.toll_honor[ach_type]:
                        progress_string = ""
                        ach_def = self.tracker.achievements_def[ach_type]
                        conditions = ach_def.conditions
                        
                        hist = list(self.history) 

                        if ach_type in {AchievementType.EMERGING_HIT, AchievementType.MEGA_HIT}:
                            window_len = conditions["weeks"]
                            rank_cond = conditions["rank"]
                            tops = hist[-window_len:]
                            ranks = []
                            for week_data in tops:
                                try:
                                    if name in week_data[:rank_cond]:
                                        ranks.append(str(week_data.index(name) + 1))
                                    else: 
                                        ranks.append("?") 
                                except ValueError:
                                    ranks.append("?") 
                            progress_string = "~".join(ranks)
                        
                        elif ach_type in {AchievementType.POTENTIAL_REGULAR, AchievementType.REGULAR}:
                            window_len = conditions["weeks"]
                            rank_cond = conditions["rank"]
                            threshold_count = conditions["threshold"]
                            slice_for_calc = hist[-window_len:]
                            
                            first_entry_offset = -1
                            for i, week_data in enumerate(slice_for_calc):
                                if name in week_data[:rank_cond]:
                                    first_entry_offset = i
                                    break
                            
                            if first_entry_offset != -1:
                                denominator = len(slice_for_calc) - first_entry_offset 
                                progress_string = f"{threshold_count}/{denominator}"
                            else:
                                progress_string = f"{threshold_count}/?"

                        title_val, bvid_val, author_val, pubdate_val = None, None, None, None
                        if not new_df.empty and 'name' in new_df.columns:
                            song_row_df = new_df[new_df['name'] == name]
                            if not song_row_df.empty:
                                details = song_row_df.iloc[0]
                                title_val = str(details.get('title', '')) if pd.notna(details.get('title')) else None
                                bvid_val = str(details.get('bvid', '')) if pd.notna(details.get('bvid')) else None
                                author_val = str(details.get('author', '')) if pd.notna(details.get('author')) else None
                                pubdate_raw = details.get('pubdate')
                                if pd.notna(pubdate_raw):
                                    try:
                                        pubdate_val = pd.to_datetime(pubdate_raw).strftime('%Y-%m-%d %H:%M:%S')
                                    except Exception:
                                        pubdate_val = str(pubdate_raw)
                        
                        ach_song_obj = AchievedSong(
                            name=name,
                            index=index,
                            honor=ach_type.value, 
                            title=title_val,
                            bvid=bvid_val,
                            author=author_val,
                            pubdate=pubdate_val
                        )
                        new_honor[ach_type].append(ach_song_obj)
                        
                        self.toll_honor[ach_type][name] = (index, progress_string)
                        new_ach_count += 1
                        logger.info(f"歌曲 '{name}' 在第 {index} 期新达成: {ach_type.value} (进度: {progress_string})")

            if new_honor:
                self.save_honor(index, new_honor)
        self._save_master_achievements()

def main():
    start_file_config = "周刊/总榜/2024-09-07.xlsx"
    folder_path_config = "周刊/总榜"
    output_folder_config = "成就/"
    start_index_config = 1
    
    target_end_index_range = range(37, 38)
    max_target_end_index_for_preload = max(target_end_index_range) if target_end_index_range else start_index_config
    
    folder_path_obj = Path(folder_path_config)
    all_files_sorted = WeeklyHonor.sort_file(folder_path_obj)
    start_file_ref_name_stem = Path(start_file_config).stem
    start_file_date_obj = datetime.strptime(start_file_ref_name_stem, "%Y-%m-%d")
    start_file_idx_in_list = next(i for i, (_, date) in enumerate(all_files_sorted) if date == start_file_date_obj)

    preloaded_weekly_data: Dict[int, Tuple[List[str], pd.DataFrame]] = {}
    actual_latest_available_idx = start_index_config + (len(all_files_sorted) - 1 - start_file_idx_in_list)
    
    preload_end_idx = min(max_target_end_index_for_preload, actual_latest_available_idx)

    logger.info(f"开始预加载周刊数据从第 {start_index_config} 期到第 {preload_end_idx} 期...")
    for week_num_to_preload in range(start_index_config, preload_end_idx + 1):
        list_offset = start_file_idx_in_list + (week_num_to_preload - start_index_config)
        if 0 <= list_offset < len(all_files_sorted):
            file_to_read, _ = all_files_sorted[list_offset]
            logger.debug(f"预加载: 第 {week_num_to_preload} 期 ({file_to_read.name})")
            song_names, details_df = WeeklyHonor.weekly_processing(file_to_read)
            preloaded_weekly_data[week_num_to_preload] = (song_names, details_df)
        else:
            logger.warning(f"预加载跳过: 第 {week_num_to_preload} 期，文件索引 {list_offset} 超出范围。")
            preloaded_weekly_data[week_num_to_preload] = ([], pd.DataFrame()) 


    for target_end_index_val in target_end_index_range:
        logger.info(f"开始处理期数: {target_end_index_val}")
        processor = WeeklyHonor( 
            start_file=start_file_config,
            folder_path=folder_path_config,
            output_folder=output_folder_config, 
            start_index=start_index_config,
            preloaded_data=preloaded_weekly_data,
            target_end_index=target_end_index_val
        )
        processor.main_processing()


if __name__ == "__main__":
    main()
