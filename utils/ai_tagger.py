# src/ai_tagger.py
import asyncio
import pandas as pd
import json
import re
import yaml
from openai import AsyncOpenAI, APIStatusError
from pathlib import Path
from utils.logger import logger
from typing import List, Dict, Set, Tuple 
from utils.data_handler import DataHandler
from utils.config_handler import ConfigHandler
from utils.io_utils import save_to_excel 

BATCH_SIZE = 20
MAX_RETRIES = 5
INITIAL_DELAY = 5
COLOR_YELLOW = 'FFFF00' # 黄色，用于AI收录的歌曲
COLOR_LIGHT_BLUE = 'ADD8E6' # 浅蓝色，用于预先已标注的歌曲

class AITagger:
    """使用自定义的、兼容OpenAI的API对视频数据进行筛选和标注。"""

    def __init__(self, input_file: Path, output_file: Path, config_handler: ConfigHandler):
        self.input_file = input_file
        self.output_file = output_file
        self.config_handler = config_handler 
        
        try:
            with open("config/ai.yaml", "r", encoding="utf-8") as f:
                api_config = yaml.safe_load(f)
        except FileNotFoundError:
            logger.error("配置文件 config/ai.yaml 未找到！")
            raise

        api_key = api_config.get("API_KEY")
        base_url = api_config.get("API_URL")
        self.model_name = api_config.get("API_MODEL")

        if not all([api_key, base_url, self.model_name]):
            raise ValueError("请在 config/ai.yaml 中正确设置 API_KEY, API_URL, 和 API_MODEL")

        self.client = AsyncOpenAI(base_url=base_url, api_key=api_key)
        
        self.known_synthesizers, self.known_vocals = self._load_known_tags()
        self.prompt_template = self._load_prompt_template()
        self.sem = asyncio.Semaphore(5)

    def _load_known_tags(self) -> Tuple[Set[str], Set[str]]:
        """从 '收录曲目.xlsx' 加载已知列表。"""
        data_handler = DataHandler(self.config_handler) 
        collected_songs_path = Path("收录曲目.xlsx") 
        
        if not collected_songs_path.exists():
            raise FileNotFoundError(f"错误：收录曲目文件 '{collected_songs_path}' 不存在。")

        try:
            collected_df = data_handler._read_excel(collected_songs_path, usecols_key='collected')
            
            synthesizers = set()
            vocals = set()

            if 'synthesizer' in collected_df.columns:
                for s in collected_df['synthesizer'].dropna().astype(str):
                    synthesizers.update(s.split('、')) 
            
            if 'vocal' in collected_df.columns:
                for v in collected_df['vocal'].dropna().astype(str):
                    vocals.update(v.split('、')) 
            
            synthesizers = {s.strip() for s in synthesizers if s.strip()}
            vocals = {v.strip() for v in vocals if v.strip()}

            logger.info(f"已加载 {len(synthesizers)} 个已知引擎和 {len(vocals)} 个已知歌手。")
            return synthesizers, vocals
        except Exception as e:
            raise

    def _load_prompt_template(self) -> str:
        """从文件加载提示词模板，并用已知标签列表格式化。"""
        try:
            with open("config/prompt_template.txt", "r", encoding="utf-8") as f:
                template = f.read()
            
            known_synthesizers_str = "\n".join(sorted(list(self.known_synthesizers))) if self.known_synthesizers else "无"
            known_vocals_str = "\n".join(sorted(list(self.known_vocals))) if self.known_vocals else "无"

            return template.format(
                known_synthesizers=known_synthesizers_str,
                known_vocals=known_vocals_str
            )
        except FileNotFoundError:
            raise
        except KeyError as e:
            logger.error(f"提示词模板缺少占位符: {e}。请检查 prompt_template.txt。")
            raise

    def _parse_json_from_response(self, text: str) -> dict:
        """从AI响应中提取JSON。如果无法提取，则报错。"""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))
                except json.JSONDecodeError:
                    logger.error("AI响应中包含JSON，但解析失败。")
                    raise ValueError("无法从AI响应中解析有效JSON。")
            else:
                logger.error("AI响应中未找到有效的JSON结构。")
                raise ValueError("AI响应中未找到有效的JSON结构。")

    async def _get_ai_tags_batch(self, videos_batch: List[Dict]) -> List[Dict]:
        """为一批视频调用AI API获取标注结果。"""
        input_data = {
            "videos": [
                {
                    "title": v.get('title', ''),
                    "uploader": v.get('uploader', ''),
                    "intro": str(v.get('intro', ''))[:1000],
                    "bvid": v.get('bvid', '') 
                }
                for v in videos_batch
            ]
        }
        
        user_message_content = json.dumps(input_data, ensure_ascii=False)
        
        retries = 0
        current_delay = INITIAL_DELAY
        
        while retries < MAX_RETRIES:
            try:
                async with self.sem:
                    response = await self.client.chat.completions.create(
                        model=self.model_name,
                        messages=[
                            {"role": "system", "content": self.prompt_template},
                            {"role": "user", "content": user_message_content}
                        ],
                        response_format={"type": "json_object"},
                        temperature=0.2,
                    )
                    content = response.choices[0].message.content
                    parsed_json = self._parse_json_from_response(content)
                    results = parsed_json.get("results", [])

                    if len(results) != len(videos_batch):
                        raise ValueError(
                            f"AI返回结果数量 ({len(results)}) 与请求数量 ({len(videos_batch)}) 不匹配。"
                            f"批次首个bvid: {videos_batch[0].get('bvid', 'N/A')}。"
                        )
                    
                    return results

            except APIStatusError as e:
                if e.status_code == 429:
                    logger.warning(
                        f"API速率限制 (429) 达到，将在 {current_delay} 秒后重试... (重试次数: {retries + 1}/{MAX_RETRIES})"
                    )
                    retries += 1
                    await asyncio.sleep(current_delay)
                    current_delay *= 2
                else:
                    logger.error(f"AI API调用失败 (状态码: {e.status_code})。")
                    raise e 
            except Exception as e:
                logger.error(f"AI API调用发生未知错误: {e}")
                raise e 
        
        logger.error(f"AI API调用在 {MAX_RETRIES} 次重试后仍然失败。")
        raise ConnectionError("AI API调用重试次数耗尽，未能成功获取结果。")

    async def _process_batch_task(self, df_chunk: pd.DataFrame) -> List[Dict]:
        """处理一个DataFrame块的异步任务。"""
        videos_data = df_chunk.to_dict('records')
        ai_results = await self._get_ai_tags_batch(videos_data)
        return ai_results

    def _prepare_dataframe(self) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[int, str]]:
        """
        读取Excel，处理列类型，分割已标注和待处理数据，并初始化行样式。
        返回：(原始DataFrame, 待AI处理的DataFrame, 行样式字典)
        """
        logger.info(f"正在读取文件: {self.input_file.name}")
        df = pd.read_excel(self.input_file)
        
        required_cols = ['synthesizer', 'vocal', 'type']
        for col in required_cols:
            df[col] = df.get(col, pd.NA).astype(object)

        row_styles: Dict[int, str] = {}

        already_tagged_mask = df[required_cols].notna().all(axis=1)
        df_already_tagged = df[already_tagged_mask]
        df_to_process = df[~already_tagged_mask].copy()

        logger.info(f"总计 {len(df)} 首歌曲。已标注 {len(df_already_tagged)} 首，待AI处理 {len(df_to_process)} 首。")

        for original_idx in df_already_tagged.index:
            row_styles[original_idx] = COLOR_LIGHT_BLUE
            
        return df, df_to_process, row_styles

    async def _process_untagged_data(self, df_to_process: pd.DataFrame) -> List[Dict]:
        """
        创建并执行AI批处理任务，并收集AI结果。
        返回：所有AI处理结果的列表。
        """
        if df_to_process.empty:
            return []

        tasks = []
        for i in range(0, len(df_to_process), BATCH_SIZE):
            df_chunk = df_to_process.iloc[i:i + BATCH_SIZE]
            tasks.append(self._process_batch_task(df_chunk))
        
        all_ai_results = await asyncio.gather(*tasks)
        return [item for sublist in all_ai_results for item in sublist]

    def _apply_ai_results(self, df: pd.DataFrame, df_to_process: pd.DataFrame, 
                          all_ai_results: List[Dict], row_styles: Dict[int, str]):
        """
        将AI返回的结果应用回主DataFrame，并更新行样式。
        """
        required_cols = ['synthesizer', 'vocal', 'type']

        if len(all_ai_results) != len(df_to_process):
             raise ValueError("AI结果数量与待处理歌曲总数不匹配，数据更新可能不准确。")

        for index_in_processed_df, ai_result in enumerate(all_ai_results):
            original_index = df_to_process.iloc[index_in_processed_df].name 
            
            if ai_result.get('include'):
                tags = ai_result.get('tags', {})
                for key, value in tags.items():
                    if key in df.columns:
                        df.at[original_index, key] = value
                row_styles[original_index] = COLOR_YELLOW 
            else:
                for col in required_cols:
                    df.at[original_index, col] = pd.NA 

    async def run(self):
        """执行完整的AI标注流程。"""
        if not self.input_file.exists():
            raise FileNotFoundError(f"输入文件不存在: {self.input_file.name}")

        df, df_to_process, row_styles = self._prepare_dataframe()

        if df_to_process.empty:
            save_to_excel(df, self.output_file, row_styles=row_styles)
            return

        logger.info(f"开始AI处理 {len(df_to_process)} 首待标注歌曲...")
        all_ai_results = await self._process_untagged_data(df_to_process)

        self._apply_ai_results(df, df_to_process, all_ai_results, row_styles)
        save_to_excel(df, self.output_file, row_styles=row_styles)

