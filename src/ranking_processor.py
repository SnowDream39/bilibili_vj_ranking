# src/ranking_processor.py
import asyncio
import pandas as pd
from datetime import datetime, timedelta

from utils.logger import logger
from utils.config_handler import ConfigHandler
from utils.data_handler import DataHandler
from utils.calculator import calculate_ranks, merge_duplicate_names, update_rank_and_rate, update_count
from utils.processing import process_records

class RankingProcessor:
    """
    负责生成和处理不同类型（如周刊、日刊、特刊）的排行榜数据。
    """
    def __init__(self, period: str):
        """初始化排行榜处理器。"""
        self.config = ConfigHandler(period)
        self.data_handler = DataHandler(self.config)
        self._dispatch_map = {
            'weekly': self.run_periodic_ranking,
            'monthly': self.run_periodic_ranking,
            'annual': self.run_periodic_ranking,
            'daily': self.run_daily_diff_async,
            'daily_combination': self.run_combination,
            'daily_new_song': self.run_daily_new_song,
            'special': self.run_special,
            'history': self.run_history
        }

    async def run(self, **kwargs):
        """根据不同的榜单类型，动态分发到相应的主处理流程。"""
        period = self.config.period
        handler_method = self._dispatch_map.get(period)
        
        if handler_method:
            # 校验不同模式下必需的参数
            if period in ['weekly', 'monthly', 'annual', 'history']:
                if 'dates' not in kwargs: raise ValueError(f"'{period}' 模式需要 'dates' 参数。")
            elif period == 'special':
                if 'song_data' not in kwargs: raise ValueError(f"'{period}' 模式需要 'song_data' 参数。")

            # 根据方法是同步还是异步，选择不同的执行方式
            if asyncio.iscoroutinefunction(handler_method):
                await handler_method(**kwargs)
            else:
                handler_method(**kwargs)
        else:
            raise ValueError(f"未知的任务类型: {period}")

    def run_periodic_ranking(self, dates: dict):
        """执行期刊（周刊/月刊/年刊）的生成流程。"""
        old_data = self.data_handler.load_merged_data(date=dates['old_date'])
        new_data = self.data_handler.load_toll_data(date=dates['new_date'])
        
        # 核心处理：计算分数
        df = process_records(
            new_data=new_data, old_data=old_data, use_old_data=True,
            old_time_toll=dates['old_date'],
            ranking_type=self.config.config['ranking_type']
        )
        
        # 去重：对于同名歌曲，只保留分数最高的一条记录
        toll_ranking = df.loc[df.groupby('name')['point'].idxmax()].reset_index(drop=True)
        toll_ranking = calculate_ranks(toll_ranking)
        
        # 根据配置，更新上榜次数、排名及升降浮动
        update_opts = self.config.config.get('update_options', {})
        if update_opts and any(update_opts.values()):
            previous_report_path = self.config.get_path('toll_ranking', 'output_paths', target_date=dates['previous_date'])
            if update_opts.get('count'):
                toll_ranking = update_count(toll_ranking, previous_report_path)
            if update_opts.get('rank_and_rate'):
                toll_ranking = update_rank_and_rate(toll_ranking, previous_report_path)
        
        # 保存总榜
        toll_ranking_path = self.config.get_path('toll_ranking', 'output_paths', target_date=dates['target_date'])
        self.data_handler.save_df(toll_ranking, toll_ranking_path, 'final_ranking')
        
        # 如果配置了生成新曲榜，则调用相应方法
        if self.config.config.get('has_new_ranking'):
            self.generate_new_ranking(toll_ranking, dates)

    def generate_new_ranking(self, toll_ranking: pd.DataFrame, dates: dict):
        """从总榜数据中筛选并生成新曲榜。"""
        on_board_names = set()
        period = self.config.period
        start_date = datetime.strptime(dates['old_date'], "%Y%m%d")
        end_date = datetime.strptime(dates['new_date'], "%Y%m%d")

        if period == 'weekly':
            # 周刊：之前上过榜的歌曲（count>0）不计为新曲
            if 'count' in toll_ranking.columns:
                on_board_names = set(toll_ranking[toll_ranking['count'] > 0]['name'])
            # 周刊的新曲时间范围向前扩展7天，即14天内的新曲
            start_date = start_date - timedelta(days=7)
        elif period == 'monthly':
            # 月刊：之前进入过前20名的歌曲不计为新曲
            if 'rank' in toll_ranking.columns:
                on_board_names = set(toll_ranking[toll_ranking['rank'] <= 20]['name'])
        
        # 创建筛选条件：在指定投稿时间范围内，且歌曲未被计为“已上榜”
        mask = (
            (pd.to_datetime(toll_ranking['pubdate']) >= start_date) &
            (pd.to_datetime(toll_ranking['pubdate']) < end_date) &
            (~toll_ranking['name'].isin(on_board_names))
        )
        new_ranking = toll_ranking[mask].copy()

        # 如果有符合条件的新曲，则计算排名并保存
        if not new_ranking.empty:
            new_ranking = calculate_ranks(new_ranking)
            new_ranking_path = self.config.get_path('new_ranking', 'output_paths', target_date=dates['target_date'])
            self.data_handler.save_df(new_ranking, new_ranking_path, 'final_ranking')

    def run_combination(self):
        """执行每日数据的合并与更新流程。"""
        dates = self.config.get_daily_new_song_dates()
        raw_combined_df = self._load_and_combine_diffs(dates)
        collected_path = self.config.get_path('collected_songs', 'input_paths')
        existing_collected_df = pd.read_excel(collected_path)
        raw_combined_df = self._resolve_name_conflicts(raw_combined_df, existing_collected_df)
        updated_collected_df = self._update_collected_songs(raw_combined_df, existing_collected_df)
        self._process_and_save_combined_ranking(raw_combined_df, dates)
        self._update_master_data_for_next_day(dates, updated_collected_df)

    def _load_and_combine_diffs(self, dates: dict) -> pd.DataFrame:
        """加载并合并新旧曲的日增数据。"""
        main_diff_path = self.config.get_path('main_diff', 'input_paths', **dates)
        new_song_diff_path = self.config.get_path('new_song_diff', 'input_paths', **dates)
        df_main_diff = pd.read_excel(main_diff_path)
        df_new_song_diff = pd.read_excel(new_song_diff_path)
        
        merged_df = pd.merge(df_new_song_diff, df_main_diff, on='bvid', how='outer', suffixes=('_new', '_main'))
        all_cols = df_main_diff.columns.union(df_new_song_diff.columns).drop('bvid')
        for col in all_cols:
            new_col = f"{col}_new"
            main_col = f"{col}_main"

            if new_col in merged_df.columns and main_col in merged_df.columns:
                merged_df[col] = merged_df[new_col].combine_first(merged_df[main_col])
                merged_df.drop(columns=[new_col, main_col], inplace=True)
            
            elif new_col in merged_df.columns:
                merged_df.rename(columns={new_col: col}, inplace=True)
            
            elif main_col in merged_df.columns:
                merged_df.rename(columns={main_col: col}, inplace=True)
        
        final_cols_order = ['bvid'] + [col for col in df_main_diff.columns if col != 'bvid' and col in merged_df.columns]
        final_cols_order += [col for col in df_new_song_diff.columns if col not in final_cols_order and col in merged_df.columns]
        return merged_df[final_cols_order]

    def _resolve_name_conflicts(self, df_to_check: pd.DataFrame, collected_df: pd.DataFrame) -> pd.DataFrame:
        """
        检查并解决同名但作者不同的歌曲冲突。
        
        Args:
            df_to_check (pd.DataFrame): 待检查的DataFrame（通常是日增数据）。
            collected_df (pd.DataFrame): 权威的收录曲目DataFrame。

        Returns:
            pd.DataFrame: 处理完名称冲突后的DataFrame。
        """
        author_map = collected_df.drop_duplicates(subset=['name'], keep='first').set_index('name')['author'].to_dict()

        def get_new_name(row):
            """应用于每一行的逻辑函数"""
            name = row['name']
            author = row['author']
            if name in author_map and str(author_map[name]).strip().lower() != str(author).strip().lower():
                new_name = f"{name}({author})"
                logger.info(f"检测到同名冲突: 原名='{name}', 作者='{author}' (收录作者='{author_map[name]}')。重命名为: '{new_name}'")
                return new_name
            return name

        df_to_check.loc[:, 'name'] = df_to_check.apply(get_new_name, axis=1)
        
        return df_to_check
    
    def _update_collected_songs(self, df: pd.DataFrame, existing_collected_df: pd.DataFrame = None) -> pd.DataFrame:
        """更新收录曲目列表。"""
        if existing_collected_df is None:
            collected_path = self.config.get_path('collected_songs', 'input_paths')
            existing_collected_df = pd.read_excel(collected_path)
        metadata_cols = self.data_handler.usecols.get('metadata_update_cols', [])
        latest_metadata = df[['bvid'] + [col for col in metadata_cols if col in df.columns]].copy()
        latest_metadata = latest_metadata.drop_duplicates(subset=['bvid'], keep='last')
        
        existing_collected_df.set_index('bvid', inplace=True)
        latest_metadata.set_index('bvid', inplace=True)
        existing_collected_df.update(latest_metadata)
        existing_collected_df.reset_index(inplace=True)
        new_songs_bvid = df[~df['bvid'].isin(existing_collected_df['bvid'])]['bvid'].unique()
        
        if len(new_songs_bvid) > 0:
            record_cols = self.data_handler.usecols.get('record', [])
            new_songs_df = df[df['bvid'].isin(new_songs_bvid)].copy()
            new_songs_df = new_songs_df.drop_duplicates(subset=['bvid'], keep='last')
            new_songs_df['streak'] = 0
            new_songs_to_add = new_songs_df[[col for col in record_cols if col in new_songs_df.columns]]
            
            updated_df = pd.concat([existing_collected_df, new_songs_to_add], ignore_index=True)
        else:
            updated_df = existing_collected_df

        output_path = self.config.get_path('collected_songs', 'output_paths')
        self.data_handler.save_df(updated_df, output_path, 'record')
        return updated_df
    
    def _process_and_save_combined_ranking(self, df: pd.DataFrame, dates: dict):
        """处理合并榜单的排名、在榜次数等并保存。"""
        merged_df = merge_duplicate_names(df)
        ranked_df = calculate_ranks(merged_df)
        
        # 基于上一期的合并榜单数据，更新在榜次数和排名变化
        old_df_path = self.config.get_path('previous_combined', 'input_paths', **dates)
        processed_df = update_count(ranked_df, old_df_path)
        processed_df = update_rank_and_rate(processed_df, old_df_path)
        
        output_path = self.config.get_path('combined_ranking', 'output_paths', **dates)
        self.data_handler.save_df(processed_df, output_path, 'final_ranking')

    def _update_master_data_for_next_day(self, dates: dict, df_collected: pd.DataFrame):
        """将新曲数据合并到主数据文件中。"""
        main_data_path = self.config.get_path('main_data', 'input_paths', **dates)
        new_song_data_path = self.config.get_path('new_song_data', 'input_paths', **dates)
        df_main = pd.read_excel(main_data_path)
        df_new_song = pd.read_excel(new_song_data_path)
        
        promotable_songs = pd.merge(df_new_song, df_collected[['bvid']], on='bvid', how='inner')
        stat_cols = self.data_handler.usecols.get('stat', [])
        df_promoted = promotable_songs[[col for col in stat_cols if col in promotable_songs.columns]].copy()
        
        updated_main_df = pd.concat([df_main, df_promoted], ignore_index=True)
        updated_main_df = updated_main_df.drop_duplicates(subset=['bvid'], keep='last').reset_index(drop=True)
        update_cols = self.data_handler.usecols.get('metadata_update_cols', [])
        update_source = df_collected[['bvid'] + [col for col in update_cols if col in df_collected.columns]].copy()

        cols_to_drop = [col for col in update_cols if col in updated_main_df.columns]
        base_df = updated_main_df.drop(columns=cols_to_drop)
        final_df = pd.merge(base_df, update_source, on='bvid', how='left')

        output_path = self.config.get_path('main_data', 'output_paths', **dates)
        self.data_handler.save_df(final_df, output_path, 'stat')

    def run_daily_new_song(self):
        """处理每日新曲榜数据。"""
        dates = self.config.get_daily_new_song_dates()
        diff_file_path = self.config.get_path('diff_file', 'input_paths', **dates)
        previous_rank_path = self.config.get_path('previous_ranking', 'input_paths', **dates)
        
        new_ranking_df = pd.read_excel(diff_file_path)
        previous_ranking_df = pd.read_excel(previous_rank_path)[['name', 'rank']]
        
        new_ranking_df = merge_duplicate_names(new_ranking_df)
        new_ranking_df = self.filter_new_song(new_ranking_df, previous_ranking_df)
        new_ranking_df = calculate_ranks(new_ranking_df) 
        
        output_path = self.config.get_path('ranking', 'output_paths', **dates)
        self.data_handler.save_df(new_ranking_df, output_path, 'new_ranking')

    def filter_new_song(self, df: pd.DataFrame, previous_rank_df: pd.DataFrame) -> pd.DataFrame:
        """
        过滤新曲榜，只保留排名上升或新上榜的歌曲。
        """
        df = df.sort_values(by='point', ascending=False).reset_index(drop=True)
        df['rank'] = df.index + 1
        
        merged_df = df.merge(previous_rank_df, on='name', how='left', suffixes=('', '_previous'))
        # 将上一期未上榜的歌曲的排名设为一个较大的值（如1000），便于后续比较
        merged_df['rank_previous'] = merged_df['rank_previous'].fillna(1000)

        new_ranking_rows = []
        for _, row in merged_df.iterrows():
            # 只保留排名上升或新上榜的歌曲
            if row['rank'] < row['rank_previous']:
                new_ranking_rows.append(row.to_dict())
        
        if not new_ranking_rows:
            return pd.DataFrame()

        final_df = pd.DataFrame(new_ranking_rows)
        return final_df.drop(columns=['rank_previous'], errors='ignore')

    async def run_daily_diff_async(self, **kwargs):
        """异步执行每日数据（主数据和新曲）的差异计算任务。"""
        df_main_diff, df_new_song_diff = await asyncio.gather(
            self.process_diff_task_async('main'),
            self.process_diff_task_async('new_song')
        )
        return df_main_diff, df_new_song_diff
        
    async def process_diff_task_async(self, task_type: str):
        """异步处理单个差异计算任务（主数据或新曲）。"""
        dates = self.config.get_daily_dates()
        
        if task_type == 'main':
            old_path = self.config.get_path('main_data', 'input_paths', date=dates['old_date'])
            new_path = self.config.get_path('main_data', 'input_paths', date=dates['new_date'])
            output_path = self.config.get_path('main_diff', 'output_paths', **dates)
            collected_data, point_threshold = None, None
        elif task_type == 'new_song':
            old_path = self.config.get_path('new_song_data', 'input_paths', date=dates['old_date'])
            new_path = self.config.get_path('new_song_data', 'input_paths', date=dates['new_date'])
            output_path = self.config.get_path('new_song_diff', 'output_paths', **dates)
            collected_path = self.config.get_path('collected_songs', 'input_paths')
            # 使用 asyncio.to_thread 在独立的线程中执行同步的I/O操作，避免阻塞事件循环
            collected_data = await asyncio.to_thread(pd.read_excel, collected_path)
            point_threshold = self.config.config.get('threshold')
        else:
            return pd.DataFrame()
            
        # 异步读取新旧数据文件
        old_data, new_data = await asyncio.gather(
            asyncio.to_thread(pd.read_excel, old_path),
            asyncio.to_thread(pd.read_excel, new_path)
        )
        
        # 计算数据差异
        df = process_records(
            new_data=new_data, 
            old_data=old_data, 
            use_old_data=True,
            collected_data=collected_data, 
            ranking_type='daily', 
            old_time_toll=dates['old_date']
        )
        
        # 根据配置对新曲应用分数阈值过滤
        if point_threshold:
            df = df[df['point'] >= point_threshold]
        df = df.sort_values('point', ascending=False)
        
        # 异步保存结果
        await asyncio.to_thread(self.data_handler.save_df, df, output_path)
        return df

    def run_special(self, song_data: str):
        """执行特刊榜单的生成流程。"""
        input_path = self.config.get_path('input_path', 'paths', song_data=song_data)
        output_path = self.config.get_path('output_path', 'paths', song_data=song_data)
        df = pd.read_excel(input_path)
        
        processing_opts = self.config.config.get('processing_options', {})
        collected_data = pd.read_excel(processing_opts['collected_data']) if 'collected_data' in processing_opts else None
            
        df = process_records(
            new_data=df,
            ranking_type = self.config.config.get('ranking_type', 'special'),
            use_old_data = processing_opts.get('use_old_data'),
            collected_data = collected_data
        )
        
        df = merge_duplicate_names(df)
        df = calculate_ranks(df)
        
        self.data_handler.save_df(df, output_path)

    def run_history(self, dates: dict):
        """执行历史榜单的生成流程。"""
        input_path = self.config.get_path('input_path', **dates)
        df = pd.read_excel(input_path)
        # 筛选出排名进入前5的歌曲
        df = df[df['rank'] <= 5][self.data_handler.usecols['history']].copy()
        output_path = self.config.get_path('output_path', **dates)
        self.data_handler.save_df(df, output_path)
