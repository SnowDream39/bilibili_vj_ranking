# src/ranking_processor.py
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from utils.config_handler import ConfigHandler
from utils.data_handler import DataHandler
from utils.calculator import calculate_ranks, merge_duplicate_names, update_rank_and_rate, update_count
from utils.processing import process_records

class RankingProcessor:
    """
    排行榜处理器类
    负责不同类型排行榜的生成、更新和数据处理

    主要功能:
    1. 周/月刊制作
    2. 日刊制作
    3. 特刊制作

    工作流程:
    1. 数据获取
    2. 数据处理
    3. 结果输出
    """
    def __init__(self, period: str):
        """
        初始化排行榜处理器
        
        参数:
            period: 榜单周期类型
                - weekly: 周刊
                - monthly: 月刊
                - annual: 年刊
                - daily: 日刊旧曲
                - daily_combination: 日刊新旧曲合并
                - daily_new_song: 日刊新曲
                - special: 特刊
        """
        self.config = ConfigHandler(period)
        self.data_handler = DataHandler(self.config)

    async def run(self, **kwargs):
        """
        主运行函数,根据不同的榜单类型执行相应的处理流程
        
        参数:
            **kwargs: 可变关键字参数
                - dates: 期刊榜单的日期信息
                    * old_date: 上期数据日期
                    * new_date: 本期数据日期
                    * target_date: 标记命名
                - song_data: 特刊的数据文件名
        
        支持的处理类型:
        1. 期刊处理 ['weekly', 'monthly', 'annual']
           - 加载新旧数据
           - 计算排名变化
           - 生成总榜和新曲榜
        
        2. 日刊处理
           - daily: 计算每日旧曲数据差异
           - daily_combination: 合并每日数据
           - daily_new_song: 处理每日新曲
        
        3. 特刊处理
           - 自定义数据处理流程
           - 灵活的配置选项
        """
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
        执行期刊(周刊/月刊/年刊)生成流程
        
        工作流程:
        1. 数据加载
           - 读取上期合并数据
           - 读取本期榜单数据
        
        2. 数据处理
           - 计算播放等数据变化
           - 合并同名数据
           - 计算排名
        
        3. 更新处理
           - 根据配置更新连续在榜次数
           - 更新排名变化和增长率
           
        4. 新曲榜生成
           - 根据配置决定是否生成新曲榜
           - 筛选符合条件的新曲
        
        参数:
            dates: 日期信息字典
                - old_date: 统计期始端数据日期
                - new_date: 统计期末端数据日期
                - previous_date: 上期标记命名
                - target_date: 本期标记命名
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
        """
        从总榜中筛选生成新曲榜
        
        筛选规则:
        1. 周刊
           - 发布时间在指定范围内
           - 之前未上榜(count=0)的歌曲
           
        2. 月刊
           - 发布时间在指定范围内
           - 之前未进入前20名的歌曲
        
        数据处理:
        1. 时间范围确定
        2. 数据筛选
        
        参数:
            toll_ranking: 总榜数据
            dates: 日期信息字典
                - old_date: 上期数据日期
                - new_date: 本期数据日期
                - target_date: 输出文件日期
        """
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
        执行每日数据合并与更新流程
        
        工作流程:
        1. 数据读取
           - 读取日刊旧曲差异数据
           - 读取日刊新曲差异数据
           - 读取已收录曲目数据
           
        2. 数据合并
           - 合并旧曲和新曲数据
           - 更新收录曲目列表
           - 处理重复曲目数据
           
        3. 榜单处理
           - 计算新的排名
           - 更新连续在榜次数
           - 更新排名变化和增长率
           
        4. 数据更新
           - 将新曲数据合并到旧曲数据文件
           - 保存更新后的文件
           
        数据流向:
        旧曲差异 --→ 合并差异 --→ 处理后数据 --→ 更新主数据
        新曲差异 ↗       ↓          ↓
                   收录曲目列表    合并榜单
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
        """
        合并旧曲和新曲的差异数据
        
        处理流程:
        1. 合并两个数据框
        2. 根据bvid去重，按新曲数据基准
        
        参数:
            df_toll: 旧曲差异数据
            df_new: 新曲差异数据
            
        返回:
            pd.DataFrame: 合并后的差异数据
        """
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
        """
        处理特刊数据
           
        参数:
            song_data: 数据文件标识符
            
        配置选项:
        - ranking_type: 榜单类型
        - processing_options:
          * use_old_data: 是否使用历史数据
          * use_collected: 是否使用收录列表
          * collected_data: 收录列表路径
        """
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
        """
        过滤新曲榜数据,只保留排名上升或新上榜的歌曲
        
        处理流程:
        1. 数据排序
           - 按播放降序排序
           - 生成初始排名
           
        2. 排名对比
           - 与前一天排名合并
           - 对缺失排名使用1000填充
           
        3. 条件筛选
           - 计算实际排名变化
           - 只保留排名上升的歌曲
           - 动态调整最终排名
           
        参数:
            df: 当前数据
            previous_rank_df: 上期排名数据
            
        返回:
            pd.DataFrame: 过滤后的新曲榜数据
            
        排名规则:
        - 保留排名上升的歌曲
        - 动态调整排名序号
        - 移除排名未上升的歌曲
        """
        df = df.sort_values(by='point', ascending=False).reset_index(drop=True)
        df['rank'] = df.index + 1
        df = df.merge(previous_rank_df[['name', 'rank']], on='name', how='left', suffixes=('', '_previous'))
        df['rank_previous'] = df['rank_previous'].fillna(1000)
        new_ranking = []
        ignore_rank = 0
        [(row.update({'rank_previous': row['rank']-ignore_rank, 'rank': row['rank']-ignore_rank}) or new_ranking.append(row) ) if (row['rank']-ignore_rank) < row['rank_previous'] else (ignore_rank := ignore_rank+1) for _, row in df.iterrows() ]
        return pd.DataFrame(new_ranking).sort_values(by='rank').reset_index(drop=True)
    
    def run_daily_new_song(self):
        """
        处理每日新曲榜数据
        
        工作流程:
        1. 数据准备
           - 读取差异数据文件
           - 读取上期榜单数据
           
        2. 数据处理
           - 合并同名数据
           - 过滤新上榜歌曲
           - 计算新的排名
           
        3. 结果输出
           - 保存新曲榜数据
           
        排名规则:
        - 按point值降序排序
        - 比较前一天排名
        - 新上榜或排名上升的歌曲入选
        - 重新计算最终排名
        
        文件依赖:
        - 差异数据: 当日新增数据
        - 上期榜单: 用于比较排名变化
        - 输出文件: 新的榜单数据
        """
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
    