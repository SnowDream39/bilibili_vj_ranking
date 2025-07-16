# src/ranking_processor.py
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from utils.config_handler import ConfigHandler
from utils.data_handler import DataHandler
from utils.calculator import calculate_ranks, merge_duplicate_names, update_rank_and_rate, update_count
from utils.processing import process_records

class RankingProcessor:
    def __init__(self, period: str):
        self.config = ConfigHandler(period)
        self.data_handler = DataHandler(self.config)

    async def run(self, **kwargs):
        period = self.config.period
        if period in ['weekly', 'monthly', 'annual']:
            dates = kwargs.get('dates')
            self.run_periodic_ranking(dates)
        
        elif period == 'daily':
            await self.run_daily_diff_async()

        elif period == 'daily_combination':
            self.run_combination()
            
        elif period == 'daily_new_song':
            self.run_daily_new_song()

        elif period == 'special':
            song_data = kwargs.get('song_data')
            self.run_special(song_data)
            
        else:
            raise ValueError(f"未知的任务类型: {period}")
            

    def run_periodic_ranking(self, dates: dict):
        """
        执行期刊生成流程。
        """
        old_data = self.data_handler.load_merged_data(date=dates['old_date'])
        new_data = self.data_handler.load_toll_data(date=dates['new_date'])
        
        df = process_records(
            new_data=new_data, old_data=old_data, use_old_data=True,
            old_time_toll=dates['old_date'],
            ranking_type=self.config.config['ranking_type']
        )
        
        toll_ranking = df.loc[df.groupby('name')['point'].idxmax()].reset_index(drop=True)
        toll_ranking = calculate_ranks(toll_ranking)

        update_opts = self.config.config.get('update_options', {})
        if update_opts.get('count') or update_opts.get('rank_and_rate'):
            previous_report_path = self.config.get_path('toll_ranking', target_date=dates['previous_date'])
            if update_opts.get('count', False):
                toll_ranking = update_count(toll_ranking, previous_report_path)
            if update_opts.get('rank_and_rate', False):
                toll_ranking = update_rank_and_rate(toll_ranking, previous_report_path)
        
        toll_ranking_path = self.config.get_path('toll_ranking', target_date=dates['target_date'])
        self.data_handler.save_df(toll_ranking, toll_ranking_path, 'final_ranking')

        if self.config.config.get('has_new_ranking', False):
            self._generate_new_ranking(toll_ranking, dates)

    def _generate_new_ranking(self, toll_ranking: pd.DataFrame, dates: dict):
        on_board_names = set()
        period = self.config.period
        start_date = datetime.strptime(dates['old_date'], "%Y%m%d")
        end_date = datetime.strptime(dates['new_date'], "%Y%m%d")
        if period == 'weekly':
            if 'count' in toll_ranking.columns:
                on_board_names = set(toll_ranking[toll_ranking['count'] > 0]['name'])
            start_date = start_date - timedelta(days=7)
        elif period == 'monthly':
            if 'rank' in toll_ranking.columns:
                on_board_names = set(toll_ranking[toll_ranking['rank'] <= 20]['name'])
        
        mask = (
            (pd.to_datetime(toll_ranking['pubdate']) >= start_date) &
            (pd.to_datetime(toll_ranking['pubdate']) < end_date) &
            (~toll_ranking['name'].isin(on_board_names))
        )
        new_ranking = toll_ranking[mask].copy()

        if not new_ranking.empty:
            new_ranking = calculate_ranks(new_ranking)
            new_ranking_path = self.config.get_path('new_ranking', target_date=dates['target_date'])
            self.data_handler.save_df(new_ranking, new_ranking_path, 'final_ranking')
    
    def run_combination(self):
        """
        执行每日数据合并、更新。
        """
        dates = self.config.get_daily_new_song_dates()
        main_diff_path = self.config.get_path('main_diff', 'input_paths', **dates)
        new_song_diff_path = self.config.get_path('new_song_diff', 'input_paths', **dates)
        df_main_diff = pd.read_excel(main_diff_path)
        df_new_song_diff = pd.read_excel(new_song_diff_path)
        raw_combined_df = self.combine_diffs(df_main_diff, df_new_song_diff)
        collected_path = self.config.get_path('collected_songs', 'input_paths')
        existing_collected_df = pd.read_excel(collected_path)
        updated_collected_df = self.update_collected_songs(raw_combined_df, existing_collected_df)
        merged_combined_df = merge_duplicate_names(raw_combined_df)
        processed_df = self.process_combined_ranking(merged_combined_df, dates)
        combined_ranking_path = self.config.get_path('combined_ranking', 'output_paths', **dates)
        self.data_handler.save_df(processed_df, combined_ranking_path, 'final_ranking')
        main_data_path = self.config.get_path('main_data', 'input_paths', **dates)
        new_song_data_path = self.config.get_path('new_song_data', 'input_paths', **dates)
        df_main = pd.read_excel(main_data_path)
        df_new_song = pd.read_excel(new_song_data_path)
        self.update_data(dates, df_main, df_new_song, updated_collected_df)
    
    def combine_diffs(self, df_toll: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
        """合并非新曲和新曲的差异文件。"""
        combined_df = pd.concat([df_toll, df_new], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['bvid'], keep='last')
        return combined_df

    def update_collected_songs(self, df: pd.DataFrame, existing_df: pd.DataFrame):
        """更新收录曲目列表，将新上榜的歌曲添加进去。"""
        selected_cols = self.data_handler.usecols['combination_input']
        df_selected = df[selected_cols].copy()
        df_selected['streak'] = 0
        
        new_songs = df_selected[~df_selected['bvid'].isin(existing_df['bvid'])]
        updated_df = pd.concat([existing_df, new_songs], ignore_index=True)
        
        output_path = self.config.get_path('collected_songs', 'output_paths')
        self.data_handler.save_df(updated_df, output_path)

        return updated_df
    
    def process_combined_ranking(self, df: pd.DataFrame, dates: dict) -> pd.DataFrame:
        df = calculate_ranks(df)
        old_df_path = self.config.get_path('previous_combined', 'input_paths', **dates)
        df = update_count(df, old_df_path)
        df = update_rank_and_rate(df, old_df_path)
        return df

    def update_data(self, dates: dict, df_main: pd.DataFrame, df_new_song: pd.DataFrame, df_collected: pd.DataFrame):
        """将新曲数据合并到主数据文件中。"""
        merged_new = df_new_song.merge(df_collected, on='bvid', suffixes=('', '_y'))
        selected_cols = self.data_handler.usecols['rename']
        cols_map = self.data_handler.maps['rename_map']
        df_promoted = merged_new[selected_cols].rename(columns=cols_map)
        updated_main_df = pd.concat([df_main, df_promoted], ignore_index=True).drop_duplicates(subset=['bvid'], keep='last')
        
        output_path = self.config.get_path('main_data', 'output_paths', **dates)
        self.data_handler.save_df(updated_main_df, output_path)

    async def run_daily_diff_async(self):
        """
        计算非新曲和新曲的每日数据差异。
        """
        df_main_diff, df_new_song_diff = await asyncio.gather(
            self.process_diff_task_async('main'),
            self.process_diff_task_async('new_song')
        )

        return df_main_diff, df_new_song_diff
    
    async def process_diff_task_async(self, task_type: str):
        dates = self.config.get_daily_dates()
        
        if task_type == 'main':
            old_path = self.config.get_path('main_data', 'input_paths', date=dates['old_date'])
            new_path = self.config.get_path('main_data', 'input_paths', date=dates['new_date'])
            output_path = self.config.get_path('main_diff', 'output_paths', **dates)
            use_collected, collected_data, point_threshold = False, None, None
        elif task_type == 'new_song':
            old_path = self.config.get_path('new_song_data', 'input_paths', date=dates['old_date'])
            new_path = self.config.get_path('new_song_data', 'input_paths', date=dates['new_date'])
            output_path = self.config.get_path('new_song_diff', 'output_paths', **dates)
            use_collected = True
            collected_path = self.config.get_path('collected_songs', 'input_paths')
            collected_data = await asyncio.to_thread(pd.read_excel, collected_path)
            point_threshold = self.config.config.get('threshold')
        else:
            return

        old_data, new_data = await asyncio.gather(
            asyncio.to_thread(pd.read_excel, old_path),
            asyncio.to_thread(pd.read_excel, new_path)
        )

        df = process_records(
            new_data=new_data, old_data=old_data, use_old_data=True,
            use_collected=use_collected, collected_data=collected_data,
            ranking_type='daily', old_time_toll=dates['old_date']
        )
        
        if point_threshold:
            df = df[df['point'] >= point_threshold]
        df = df.sort_values('point', ascending=False)

        await asyncio.to_thread(self.data_handler.save_df, df, output_path)
        return df
    
    def run_special(self, song_data: str):
        input_path = self.config.get_path('input_path', 'paths', song_data=song_data)
        output_path = self.config.get_path('output_path', 'paths', song_data=song_data)
        df = pd.read_excel(input_path)
        processing_opts = self.config.config.get('processing_options', {})
        df = process_records(
            new_data=df,
            ranking_type = self.config.config.get('ranking_type', 'special'),
            use_old_data = processing_opts.get('use_old_data'),
            use_collected = processing_opts.get('use_collected'),
            collected_data = pd.read_excel(processing_opts.get('collected_data'))
        )
        df = merge_duplicate_names(df)
        df = calculate_ranks(df)
        
        self.data_handler.save_df(df, output_path)

    def _filter_new_song(self, df, previous_rank_df):
        df = df.sort_values(by='point', ascending=False).reset_index(drop=True)
        df['rank'] = df.index + 1
        df = df.merge(previous_rank_df[['name', 'rank']], on='name', how='left', suffixes=('', '_previous'))
        df['rank_previous'] = df['rank_previous'].fillna(1000)
        new_ranking = []
        ignore_rank = 0
        [(row.update({'rank_previous': row['rank']-ignore_rank, 'rank': row['rank']-ignore_rank}) or new_ranking.append(row) ) if (row['rank']-ignore_rank) < row['rank_previous'] else (ignore_rank := ignore_rank+1) for _, row in df.iterrows() ]
        return pd.DataFrame(new_ranking).sort_values(by='rank').reset_index(drop=True)
    
    def run_daily_new_song(self):
        dates = self.config.get_daily_new_song_dates()
        diff_file_path = self.config.get_path('diff_file', 'input_paths', **dates)
        previous_rank_path = self.config.get_path('previous_ranking', 'input_paths', **dates)
        output_path = self.config.get_path('ranking', 'output_paths', **dates)
        new_ranking_df = pd.read_excel(diff_file_path)
        previous_ranking_df = pd.read_excel(previous_rank_path)
        previous_ranking_df = previous_ranking_df[['name', 'rank']]
        new_ranking_df = merge_duplicate_names(new_ranking_df)
        new_ranking_df = self._filter_new_song(new_ranking_df, previous_ranking_df)
        new_ranking_df = calculate_ranks(new_ranking_df) 
        self.data_handler.save_df(new_ranking_df, output_path, 'new_ranking')
    
class DailyNewSongProcessor:
    """
    日刊新曲榜。
    """
    def __init__(self):
        self.config = ConfigHandler('daily_new_song')
        self.data_handler = DataHandler(self.config)

    def _filter_new_song(self, df, previous_rank_df):
        df = df.sort_values(by='point', ascending=False).reset_index(drop=True)
        df['rank'] = df.index + 1
        df = df.merge(previous_rank_df[['name', 'rank']], on='name', how='left', suffixes=('', '_previous'))
        df['rank_previous'] = df['rank_previous'].fillna(1000)
        new_ranking = []
        ignore_rank = 0
        [(row.update({'rank_previous': row['rank']-ignore_rank, 'rank': row['rank']-ignore_rank}) or new_ranking.append(row) ) if (row['rank']-ignore_rank) < row['rank_previous'] else (ignore_rank := ignore_rank+1) for _, row in df.iterrows() ]
        return pd.DataFrame(new_ranking).sort_values(by='rank').reset_index(drop=True)

    def run(self):
        dates = self.config.get_daily_new_song_dates()

        diff_file_path = self.config.get_path('diff_file', 'input_paths', **dates)
        previous_rank_path = self.config.get_path('previous_ranking', 'input_paths', **dates)
        output_path = self.config.get_path('ranking', 'output_paths', **dates)
        
        new_ranking_df = pd.read_excel(diff_file_path)
        previous_ranking_df = pd.read_excel(previous_rank_path)

        previous_ranking_df = previous_ranking_df[['name', 'rank']]
        new_ranking_df = new_ranking_df.loc[new_ranking_df.groupby('name')['point'].idxmax()].reset_index(drop=True)
        new_ranking_df = self._filter_new_song(new_ranking_df, previous_ranking_df)
        new_ranking_df = calculate_ranks(new_ranking_df) 
        self.data_handler.save_df(new_ranking_df, output_path, 'new_ranking')

