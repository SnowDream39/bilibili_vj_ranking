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
    负责生成和处理不同类型（如周刊、日刊、特刊）的排行榜数据。

    该类根据指定的榜单周期类型，执行相应的数据加载、处理、计算和保存流程。
    """
    def __init__(self, period: str):
        """
        初始化排行榜处理器
        
        Args:
            period (str): 排行榜类型。可选值:
                'weekly', 'monthly', 'annual', 'daily', 'daily_combination',
                'daily_new_song', 'special', 'history'。
        """
        self.config = ConfigHandler(period)
        self.data_handler = DataHandler(self.config)

    async def run(self, **kwargs):
        """根据不同的榜单类型，执行相应的主处理流程。

        该方法作为入口，根据初始化时设置的周期类型，分发到不同的处理函数。
        
        Args:
            **kwargs: 可变关键字参数。
                - dates (dict): 用于期刊榜单的日期信息。
                - song_data (str): 用于特刊的数据文件名。
        
        Raises:
            ValueError: 如果指定的任务类型未知。
        """
        period = self.config.period
        # 根据不同的榜单周期调用相应的处理函数
        if period in ['weekly', 'monthly', 'annual']:
            dates = kwargs.get('dates')
            if not isinstance(dates, dict):
                raise ValueError(f"'{period}' 模式需要一个 'dates' 字典参数。")
            # 处理周刊、月刊、年刊
            self.run_periodic_ranking(dates)
        
        elif period == 'daily':
            # 异步处理每日数据差异
            await self.run_daily_diff_async()

        elif period == 'daily_combination':
            # 合并每日新旧曲数据
            self.run_combination()
            
        elif period == 'daily_new_song':
            # 处理每日新曲榜
            self.run_daily_new_song()

        elif period == 'special':
            # 处理特刊
            song_data = kwargs.get('song_data')
            if not isinstance(song_data, str):
                raise ValueError("'special' 模式需要一个 'song_data' 字符串参数。")
            self.run_special(song_data)
    
        elif period == 'history':
            # 处理历史回顾
            dates = kwargs.get('dates')
            if not isinstance(dates, dict):
                raise ValueError("'history' 模式需要一个 'dates' 字典参数。")
            self.run_history(dates)
        
        else:
            raise ValueError(f"未知的任务类型: {period}")
            

    def run_periodic_ranking(self, dates: dict):
        """执行期刊（周刊/月刊/年刊）的生成流程。

        该方法加载指定周期的新旧数据，计算得分和排名变化，
        并根据配置生成总榜和新曲榜。
        
        Args:
            dates (dict): 包含统计周期起止日期和文件命名日期的字典。
                - old_date (str): 统计期始端数据日期。
                - new_date (str): 统计期末端数据日期。
                - previous_date (str): 上期文件标记。
                - target_date (str): 本期文件标记。
        """
        # 加载统计周期开始和结束时的数据
        old_data = self.data_handler.load_merged_data(date=dates['old_date'])
        new_data = self.data_handler.load_toll_data(date=dates['new_date'])
        # 处理数据记录，计算得分等
        df = process_records(
            new_data=new_data, old_data=old_data, use_old_data=True,
            old_time_toll=dates['old_date'],
            ranking_type=self.config.config['ranking_type']
        )
        # 对于同名歌曲，只保留得分最高的一条记录
        toll_ranking = df.loc[df.groupby('name')['point'].idxmax()].reset_index(drop=True)
        # 计算排名
        toll_ranking = calculate_ranks(toll_ranking)
        # 获取更新选项配置
        update_opts = self.config.config.get('update_options', {})
        if update_opts.get('count') or update_opts.get('rank_and_rate'):
            previous_report_path = self.config.get_path('toll_ranking', 'output_paths', target_date=dates['previous_date'])
            # 更新在榜次数
            if update_opts.get('count', False):
                toll_ranking = update_count(toll_ranking, previous_report_path)
            # 更新排名和增长率
            if update_opts.get('rank_and_rate', False):
                toll_ranking = update_rank_and_rate(toll_ranking, previous_report_path)
        
        # 保存总榜数据
        toll_ranking_path = self.config.get_path('toll_ranking', 'output_paths', target_date=dates['target_date'])
        self.data_handler.save_df(toll_ranking, toll_ranking_path, 'final_ranking')
        # 如果配置了生成新曲榜，则执行生成流程
        if self.config.config.get('has_new_ranking', False):
            self.generate_new_ranking(toll_ranking, dates)

    def generate_new_ranking(self, toll_ranking: pd.DataFrame, dates: dict):
        """从总榜数据中筛选并生成新曲榜。

        新曲的筛选规则根据榜单类型（周刊/月刊）有所不同，
        主要依据发布时间和历史在榜情况。
        
        Args:
            toll_ranking (pd.DataFrame): 已生成的总榜数据。
            dates (dict): 包含日期信息的字典。
        """
        on_board_names = set()
        period = self.config.period
        # 确定新曲的发布时间范围
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
        
        # 创建筛选条件：在时间范围内且未被视为“已上榜”
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
    
    def run_history(self, dates: dict):
        """处理历史回顾数据，筛选并保存指定时期的TOP5歌曲。
        
        Args:
            dates (dict): 包含日期和路径信息的字典。
        """
        input_path = self.config.get_path('input_path', **dates)
        df = pd.read_excel(input_path)
        # 筛选出排名前5的歌曲，并只保留指定的列
        df = df[df['rank'] <= 5][self.data_handler.usecols['history']].copy()
        output_path = self.config.get_path('output_path', **dates)
        self.data_handler.save_df(df, output_path)
    
    def run_combination(self):
        """执行每日数据的合并与更新流程。

        此方法负责将每日的新曲和旧曲日增数据合并，更新收录曲目列表，
        生成合并后的总榜，并为下一天的计算更新主数据文件。
        """
        dates = self.config.get_daily_new_song_dates()
        # 读取旧曲和新曲的日增数据
        main_diff_path = self.config.get_path('main_diff', 'input_paths', **dates)
        new_song_diff_path = self.config.get_path('new_song_diff', 'input_paths', **dates)
        df_main_diff = pd.read_excel(main_diff_path)
        df_new_song_diff = pd.read_excel(new_song_diff_path)
        # 合并新旧曲的日增数据
        raw_combined_df = self.combine_diffs(df_main_diff, df_new_song_diff)
        # 读取已收录的曲目列表
        collected_path = self.config.get_path('collected_songs', 'input_paths')
        existing_collected_df = pd.read_excel(collected_path)
        # 将新上榜的歌曲添加到收录列表中
        updated_collected_df = self.update_collected_songs(raw_combined_df, existing_collected_df)
        # 合并同名曲目的数据
        merged_combined_df = merge_duplicate_names(raw_combined_df)
        # 计算合并榜单的排名、在榜次数等
        processed_df = self.process_combined_ranking(merged_combined_df, dates)
        # 保存合并后的总榜
        combined_ranking_path = self.config.get_path('combined_ranking', 'output_paths', **dates)
        self.data_handler.save_df(processed_df, combined_ranking_path, 'final_ranking')
        # 读取旧曲和新曲的原始数据文件
        main_data_path = self.config.get_path('main_data', 'input_paths', **dates)
        new_song_data_path = self.config.get_path('new_song_data', 'input_paths', **dates)
        df_main = pd.read_excel(main_data_path)
        df_new_song = pd.read_excel(new_song_data_path)
        # 将新曲数据更新到主数据文件中，为下一天做准备
        self.update_data(dates, df_main, df_new_song, updated_collected_df)
    
    def combine_diffs(self, df_toll: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
        """合并旧曲和新曲的差异数据。
        
        Args:
            df_toll (pd.DataFrame): 旧曲差异数据。
            df_new (pd.DataFrame): 新曲差异数据。
            
        Returns:
            pd.DataFrame: 合并后的差异数据，对于重复的bvid，保留新曲数据。
        """
        combined_df = pd.concat([df_toll, df_new], ignore_index=True)
        # 合并后去重，如果bvid相同，保留后面的记录（即新曲数据）
        combined_df = combined_df.drop_duplicates(subset=['bvid'], keep='last')
        return combined_df

    def update_collected_songs(self, df: pd.DataFrame, existing_df: pd.DataFrame):
        """更新收录曲目列表，将新上榜的歌曲添加进去。
        
        Args:
            df (pd.DataFrame): 包含新上榜歌曲的合并差异数据。
            existing_df (pd.DataFrame): 已有的收录曲目列表。

        Returns:
            pd.DataFrame: 更新后的收录曲目列表。
        """
        # 从合并数据中选取必要的列
        selected_cols = self.data_handler.usecols['combination_input']
        df_selected = df[selected_cols].copy()
        df_selected['streak'] = 0
        # 筛选出尚未被收录的新歌曲
        new_songs = df_selected[~df_selected['bvid'].isin(existing_df['bvid'])]
        # 将新歌曲追加到已收录列表
        updated_df = pd.concat([existing_df, new_songs], ignore_index=True)
        # 保存更新后的收录曲目列表
        output_path = self.config.get_path('collected_songs', 'output_paths')
        self.data_handler.save_df(updated_df, output_path)

        return updated_df
    
    def process_combined_ranking(self, df: pd.DataFrame, dates: dict) -> pd.DataFrame:
        """处理合并后的榜单，计算排名、在榜次数和排名变化。

        Args:
            df (pd.DataFrame): 待处理的合并榜单数据。
            dates (dict): 包含日期信息的字典，用于定位上期榜单。

        Returns:
            pd.DataFrame: 处理完成的榜单数据。
        """
        # 计算排名
        df = calculate_ranks(df)
        # 读取上一期的合并榜单，用于计算在榜次数和排名变化
        old_df_path = self.config.get_path('previous_combined', 'input_paths', **dates)
        # 更新在榜次数
        df = update_count(df, old_df_path)
        # 更新排名变化和增长率
        df = update_rank_and_rate(df, old_df_path)
        return df

    def update_data(self, dates: dict, df_main: pd.DataFrame, df_new_song: pd.DataFrame, df_collected: pd.DataFrame):
        """将新曲数据合并到主数据文件中，为下一周期做准备。
        
        Args:
            dates (dict): 日期信息字典。
            df_main (pd.DataFrame): 主数据文件（旧曲）的DataFrame。
            df_new_song (pd.DataFrame): 新曲数据的DataFrame。
            df_collected (pd.DataFrame): 已收录曲目列表的DataFrame。
        """
        # 将新曲数据与收录列表合并，以获取完整的元数据
        merged_new = df_new_song.merge(df_collected, on='bvid', suffixes=('', '_y'))
        # 选取需要的列，并重命名以匹配主数据文件的格式
        selected_cols = self.data_handler.usecols['rename']
        cols_map = self.data_handler.maps['rename_map']
        df_promoted = merged_new[selected_cols].rename(columns=cols_map)
        # 将处理后的新曲数据追加到主数据文件中，并去重
        updated_main_df = pd.concat([df_main, df_promoted], ignore_index=True).drop_duplicates(subset=['bvid'], keep='last')
        # 保存更新后的主数据文件
        output_path = self.config.get_path('main_data', 'output_paths', **dates)
        self.data_handler.save_df(updated_main_df, output_path)

    async def run_daily_diff_async(self):
        """异步计算旧曲和新曲的每日数据差异。
        
        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: 分别包含旧曲和新曲差异数据的DataFrame元组。
        """
        # 并发执行旧曲和新曲的日增数据计算任务
        df_main_diff, df_new_song_diff = await asyncio.gather(
            self.process_diff_task_async('main'),
            self.process_diff_task_async('new_song')
        )

        return df_main_diff, df_new_song_diff
    
    async def process_diff_task_async(self, task_type: str):
        """异步处理单个差异计算任务（旧曲或新曲）。

        Args:
            task_type (str): 任务类型，'main'为旧曲，'new_song'为新曲。

        Returns:
            pd.DataFrame: 计算出的差异数据。
        """
        dates = self.config.get_daily_dates()
        
        if task_type == 'main':
            # 旧曲任务的路径和参数
            old_path = self.config.get_path('main_data', 'input_paths', date=dates['old_date'])
            new_path = self.config.get_path('main_data', 'input_paths', date=dates['new_date'])
            output_path = self.config.get_path('main_diff', 'output_paths', **dates)
            collected_data, point_threshold = None, None
        elif task_type == 'new_song':
            # 新曲任务的路径和参数
            old_path = self.config.get_path('new_song_data', 'input_paths', date=dates['old_date'])
            new_path = self.config.get_path('new_song_data', 'input_paths', date=dates['new_date'])
            output_path = self.config.get_path('new_song_diff', 'output_paths', **dates)
            collected_path = self.config.get_path('collected_songs', 'input_paths')
            # 使用异步线程读取Excel，避免阻塞事件循环
            collected_data = await asyncio.to_thread(pd.read_excel, collected_path)
            point_threshold = self.config.config.get('threshold')
        else:
            return
        # 并发读取新旧数据文件
        old_data, new_data = await asyncio.gather(
            asyncio.to_thread(pd.read_excel, old_path),
            asyncio.to_thread(pd.read_excel, new_path)
        )
        # 调用核心处理函数，计算得分
        df = process_records(
            new_data=new_data, old_data=old_data, use_old_data=True,
            collected_data=collected_data,
            ranking_type='daily', old_time_toll=dates['old_date']
        )
        # 如果设置了得分阈值，则进行筛选
        if point_threshold:
            df = df[df['point'] >= point_threshold]
        df = df.sort_values('point', ascending=False)
        # 异步保存结果
        await asyncio.to_thread(self.data_handler.save_df, df, output_path)
        return df
    
    def run_special(self, song_data: str):
        """处理特刊数据。

        根据配置文件中的定义，对指定的输入数据进行处理，生成特刊榜单。
           
        Args:
            song_data (str): 标识特刊数据文件名的字符串。
        """
        # 根据配置获取输入输出路径
        input_path = self.config.get_path('input_path', 'paths', song_data=song_data)
        output_path = self.config.get_path('output_path', 'paths', song_data=song_data)
        df = pd.read_excel(input_path)
        # 获取特刊的处理选项
        processing_opts = self.config.config.get('processing_options', {})
        # 根据配置调用核心处理函数
        df = process_records(
            new_data=df,
            ranking_type = self.config.config.get('ranking_type', 'special'),
            use_old_data = processing_opts.get('use_old_data'),
            collected_data = pd.read_excel(processing_opts.get('collected_data'))
        )
        # 合并同名曲目并计算排名
        df = merge_duplicate_names(df)
        df = calculate_ranks(df)
        
        self.data_handler.save_df(df, output_path)

    def filter_new_song(self, df: pd.DataFrame, previous_rank_df: pd.DataFrame):
        """过滤新曲榜数据，只保留排名上升或新上榜的歌曲，并重新计算排名。
        
        Args:
            df (pd.DataFrame): 当前的新曲数据。
            previous_rank_df (pd.DataFrame): 上一期的排名数据，用于对比。
            
        Returns:
            pd.DataFrame: 过滤并重新排名后的新曲榜数据。
        """
        # 按得分排序并生成临时排名
        df = df.sort_values(by='point', ascending=False).reset_index(drop=True)
        df['rank'] = df.index + 1
        # 合并上一期的排名数据
        df = df.merge(previous_rank_df[['name', 'rank']], on='name', how='left', suffixes=('', '_previous'))
        # 对于新上榜的歌曲，将其上一期排名设为一个很大的数（1000），以确保它们被保留
        df['rank_previous'] = df['rank_previous'].fillna(1000)
        new_ranking = []
        ignore_rank = 0 # 记录因排名下降而被忽略的歌曲数量
        # 这是一个非常紧凑的列表推导式，用于筛选和重新计算排名
        # 遍历每一行，判断调整后的新排名是否优于旧排名
        # 如果排名上升或新上榜，则更新其排名并加入新列表
        # 如果排名下降或不变，则增加ignore_rank计数器，这会动态地使后续歌曲的排名标准更宽松
        [(row.__setitem__('rank_previous', row['rank'] - ignore_rank) or row.__setitem__('rank', row['rank'] - ignore_rank) or new_ranking.append(row) ) if (row['rank']-ignore_rank) < row['rank_previous'] else (ignore_rank := ignore_rank+1) for _, row in df.iterrows() ]
        return pd.DataFrame(new_ranking).sort_values(by='rank').reset_index(drop=True)
    
    def run_daily_new_song(self):
        """处理每日新曲榜数据。

        该方法读取日增数据和上期榜单，通过比较排名变化，筛选出新上榜
        或排名上升的歌曲，并生成最终的新曲榜单。
        """
        dates = self.config.get_daily_new_song_dates()
        # 读取新曲的日增数据和上一期的排名数据
        diff_file_path = self.config.get_path('diff_file', 'input_paths', **dates)
        previous_rank_path = self.config.get_path('previous_ranking', 'input_paths', **dates)
        output_path = self.config.get_path('ranking', 'output_paths', **dates)
        new_ranking_df = pd.read_excel(diff_file_path)
        previous_ranking_df = pd.read_excel(previous_rank_path)
        # 只保留上一期排名的name和rank列
        previous_ranking_df = previous_ranking_df[['name', 'rank']]
        # 合并同名曲目
        new_ranking_df = merge_duplicate_names(new_ranking_df)
        # 过滤掉排名下降的歌曲
        new_ranking_df = self.filter_new_song(new_ranking_df, previous_ranking_df)
        # 重新计算最终排名
        new_ranking_df = calculate_ranks(new_ranking_df) 
        # 保存最终的新曲榜
        self.data_handler.save_df(new_ranking_df, output_path, 'new_ranking')
    
