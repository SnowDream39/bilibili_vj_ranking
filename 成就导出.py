from dataclasses import dataclass
from typing import List, Dict, Tuple, Set, Optional
import pandas as pd
from collections import deque, defaultdict
from datetime import datetime
from pathlib import Path
from enum import Enum
from utils.logger import logger

@dataclass
class AchiDef:
   name: str
   conditions: dict

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
       self.defs: Dict[AchiType, AchiDef] = {
           AchiType.EMERGING_HIT: AchiDef(
               name="Emerging Hit!",
               conditions={"weeks": 3, "rank": 5}
           ),
           AchiType.MEGA_HIT: AchiDef(
               name="Mega Hit!!!",
               conditions={"weeks": 5, "rank": 3}
           ),
           AchiType.POTENTIAL_REGULAR: AchiDef(
               name="门番候补",
               conditions={"weeks": 15, "rank": 20, "threshold": 10}
           ),
           AchiType.REGULAR: AchiDef(
               name="门番",
               conditions={"weeks": 30, "rank": 20, "threshold": 20}
           )
       }
       self.hist_len = max(d.conditions.get("weeks", 1) for d in self.defs.values()) if self.defs else 0

   def detect(self, history: deque, song_name: str) -> Set[AchiType]:
       res = set()
       h_list = list(history)

       for atype, adef in self.defs.items():
           conds = adef.conditions
           weeks = conds["weeks"]
           h_slice = h_list[-weeks:]

           if atype in {AchiType.EMERGING_HIT, AchiType.MEGA_HIT}:
               if len(h_slice) == weeks and \
                  all(song_name in week_data[:conds["rank"]] for week_data in h_slice if week_data):
                   res.add(atype)
           else:
               top_count = sum(song_name in week_data[:conds["rank"]] for week_data in h_slice if week_data)
               if top_count >= conds["threshold"]:
                   res.add(atype)
       return res

class WeeklyHonor:
   def __init__(self,
                out_dir: str,
                load_s: int,
                data: Dict[int, Tuple[List[str], pd.DataFrame, str]],
                target_p: int,
                hist_win: int):

       self.out_dir = Path(out_dir)
       self.out_dir.mkdir(parents=True, exist_ok=True)
       self.load_s = load_s
       self.target_p = target_p
       self.tracker = Achievement()
       self.hist_q: deque[List[str]] = deque(maxlen=hist_win)
       self.master_f = self.out_dir / "成就.xlsx"
       self.all_achis: Dict[AchiType, Dict[str, Tuple[int, str]]] = self.load_all()
       self.data = data

   def load_all(self) -> Dict[AchiType, Dict[str, Tuple[int, str]]]:
       loaded = defaultdict(dict)
       if not self.master_f.exists():
           return loaded
       xls = pd.ExcelFile(self.master_f, engine='openpyxl')
       for atype in AchiType:
           sheet = atype.value
           if sheet in xls.sheet_names:
               df = pd.read_excel(xls, sheet_name=sheet)
               for _, row in df.iterrows():
                   loaded[atype][str(row['name'])] = (int(row['index']), str(row.get('progress', '')))
       return loaded

   def _save_all(self):
       with pd.ExcelWriter(self.master_f, engine='openpyxl') as writer:
           for atype, s_map in self.all_achis.items():
               if s_map:
                   rows = [{'name': name, 'index': idx, 'progress': progress}
                              for name, (idx, progress) in sorted(s_map.items(), key=lambda item: (item[1][0], item[0]))]
                   df = pd.DataFrame(rows)
                   df.to_excel(writer, sheet_name=atype.value, index=False)

   def save_report(self, idx: int, date_s: str, report_data: Dict[AchiType, List[AchievedSong]]):
       out_f = self.out_dir / f"成就{date_s}.xlsx"
       with pd.ExcelWriter(out_f, engine='openpyxl') as writer:
           for atype in AchiType:
               s_list = report_data.get(atype, [])
               rows = []
               if s_list:
                   s_sorted = sorted(s_list, key=lambda s: s.name)
                   for s in s_sorted:
                       rows.append({
                           'title': s.title or '', 'bvid': s.bvid or '',
                           'name': s.name, 'author': s.author or '',
                           'pubdate': s.pubdate or '', 'honor': s.honor
                       })
               df = pd.DataFrame(rows)
               exp_cols = ['title', 'bvid', 'name', 'author', 'pubdate', 'honor']
               for col in exp_cols:
                   if col not in df.columns:
                       df[col] = None
               df = df[exp_cols]
               df.to_excel(writer, sheet_name=atype.value, index=False)
       logger.info(f"已保存成就: {out_f.name}")


   @staticmethod
   def sort_files(dir_p: Path) -> List[Tuple[Path, datetime]]:
       f_dates = []
       for f_p in sorted(dir_p.glob("*.xlsx")):
           date_s = f_p.stem
           if " " in date_s:
               date_s = date_s.split(" ")[-1]
           date = datetime.strptime(date_s, "%Y-%m-%d")
           f_dates.append((f_p, date))
       f_dates.sort(key=lambda x: (x[1], x[0].stem))
       return f_dates

   @staticmethod
   def proc_file(f_p: Path) -> Tuple[List[str], pd.DataFrame]:
       df = pd.read_excel(f_p, engine='openpyxl')
       req_cols = ['name', 'title', 'bvid', 'author', 'pubdate']
       top = df.head(20).copy()
       names = []
       for idx in top.index:
           n_val = top.loc[idx, 'name']
           n_proc = str(n_val).strip() if pd.notna(n_val) else ""
           names.append(n_proc)
           top.loc[idx, 'name'] = n_proc

       details = top[req_cols]
       return names, details

   def run(self):
       final_achis: Dict[AchiType, List[AchievedSong]] = defaultdict(list)

       for curr_p in range(self.load_s, self.target_p + 1):
           p_names, p_details, p_date_s = self.data[curr_p]
           self.hist_q.append(p_names)
           if curr_p < self.target_p:
               continue
           hist_songs = set(s_name for week_list in self.hist_q for s_name in week_list if s_name)
           for name in hist_songs:
               if not name: continue
               song_types = self.tracker.detect(self.hist_q, name)

               for atype in song_types:
                   if name not in self.all_achis[atype]:
                       prog_s = ""
                       adef = self.tracker.defs[atype]
                       conds = adef.conditions
                       hist_list = list(self.hist_q)

                       if atype in {AchiType.EMERGING_HIT, AchiType.MEGA_HIT}:
                           win, rank_c = conds["weeks"], conds["rank"]
                           ranks = [str(wd.index(name) + 1) if name in wd[:rank_c] else ("X" if name in wd else "-")
                                    for wd in hist_list[-win:]]
                           prog_s = "~".join(ranks)
                       elif atype in {AchiType.POTENTIAL_REGULAR, AchiType.REGULAR}:
                           win, rank_c, thresh = conds["weeks"], conds["rank"], conds["threshold"]
                           calc_slice = hist_list[-win:]
                           count = sum(name in wd[:rank_c] for wd in calc_slice if wd)
                           eff_w = min(len(hist_list), win)
                           prog_s = f"{count}/{eff_w} (目标:{thresh}/{win})"

                       s_title, s_bvid, s_author, s_pdate = None, None, None, None
                       if not p_details.empty:
                           s_row = p_details[p_details['name'] == name]
                           if not s_row.empty:
                               row_data = s_row.iloc[0]
                               s_title = str(row_data.get('title', ''))
                               s_bvid = str(row_data.get('bvid', ''))
                               s_author = str(row_data.get('author', ''))
                               raw_pdate = row_data.get('pubdate')
                               s_pdate = pd.to_datetime(raw_pdate).strftime('%Y-%m-%d %H:%M:%S') if pd.notna(raw_pdate) else str(raw_pdate)

                       new_asong = AchievedSong(name, curr_p, atype.value, s_title, s_bvid, s_author, s_pdate)
                       final_achis[atype].append(new_asong)
                       self.all_achis[atype][name] = (curr_p, prog_s)
                       logger.info(f"新成就: '{name}', 期{curr_p}, {atype.value}, {prog_s}")

           self.save_report(curr_p, p_date_s, final_achis)

       self._save_all()

def main():
   START_IDX = 1
   START_FILE = "2024-09-07.xlsx"
   IN_DIR = "周刊/总榜"
   OUT_DIR = "成就/"

   cfg_track = Achievement()
   hist_win_cfg = cfg_track.hist_len

   in_p = Path(IN_DIR)
   out_p = Path(OUT_DIR)
   out_p.mkdir(parents=True, exist_ok=True)

   all_f = WeeklyHonor.sort_files(in_p)
   f_stem = Path(START_FILE).stem
   if " " in f_stem:
           f_stem = f_stem.split(" ")[-1]

   avail_idx = next(i for i, (path, dt) in enumerate(all_f)
                              if dt.strftime("%Y-%m-%d") == f_stem)
   latest_p = START_IDX + (len(all_f) - 1 - avail_idx)
   target_p_main = latest_p
   load_s_main = max(START_IDX, target_p_main - hist_win_cfg + 1)
   load_e_main = target_p_main

   logger.info(f"目标期数: {target_p_main}")

   main_data: Dict[int, Tuple[List[str], pd.DataFrame, str]] = {}
   for p_load in range(load_s_main, load_e_main + 1):
       f_idx = avail_idx + (p_load - START_IDX)
       f_read, f_date = all_f[f_idx]
       date_s = f_date.strftime("%Y-%m-%d")
       names, details = WeeklyHonor.proc_file(f_read)
       main_data[p_load] = (names, details, date_s)

   proc = WeeklyHonor(
       out_dir=OUT_DIR,
       load_s=load_s_main,
       data=main_data,
       target_p=target_p_main,
       hist_win=hist_win_cfg
   )
   proc.run()

if __name__ == "__main__":
   main()
