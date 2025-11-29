# utils/ai_tagger.py
import asyncio
import pandas as pd
import json
import re
import yaml
from openai import AsyncOpenAI, Timeout 
from pathlib import Path
from utils.logger import logger
from typing import List, Dict, Set, Tuple, Optional
from utils.data_handler import DataHandler
from utils.config_handler import ConfigHandler
from utils.io_utils import save_to_excel 

COLOR_YELLOW = 'FFFF00' # 黄色，用于AI收录的歌曲
COLOR_LIGHT_BLUE = 'ADD8E6' # 浅蓝色，用于预先已标注的歌曲

class AITagger:
    """使用远程API对视频数据进行筛选和标注。"""

    def __init__(self, input_file: Path, output_file: Path, config_handler: ConfigHandler):
        self.input_file = input_file
        self.output_file = output_file
        self.config_handler = config_handler 
        
        try:
            with open("config/ai.yaml", "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
        except FileNotFoundError:
            logger.error("配置文件 config/ai.yaml 未找到！")
            raise

        self.provider = cfg.get('PROVIDER', 'API')
        self.batch_size = cfg.get("BATCH_SIZE", 15) 
        self.max_concurrency = cfg.get("MAX_CONCURRENCY", 10) 
        self.max_attempts = cfg.get("MAX_ATTEMPTS", 10)
        
        if self.provider == 'LOCAL':
            provider_cfg = cfg.get('LOCAL', {})
            logger.info("AI Tagger 正在使用本地模型提供商 (LOCAL)。")
        elif self.provider == 'API':
            provider_cfg = cfg.get('API', {})
            logger.info("AI Tagger 正在使用远程 API 提供商 (API)。")
        else:
            raise ValueError(f"config/ai.yaml 中的 AI 提供商 '{self.provider}' 不支持。请使用 'API' 或 'LOCAL'。")

        self.model_name = provider_cfg.get("API_MODEL")
        if not self.model_name:
            raise ValueError(f"未在 config/ai.yaml 中为提供商 '{self.provider}' 定义 API_MODEL。")

        self.client = AsyncOpenAI(
            base_url=provider_cfg.get("API_URL"), 
            api_key=provider_cfg.get("API_KEY"), 
            timeout=Timeout(60.0)
        )
        
        self.semaphore = asyncio.Semaphore(self.max_concurrency)
        synthesizers, vocals = self._load_known_tags()
        self.prompt_template = self._load_prompt_template(synthesizers, vocals)

    def _extract_tags(self, series: pd.Series) -> Set[str]:
        """提取并清洗标签集合。"""
        tags = set()
        for item in series.dropna().astype(str):
            tags.update(tag.strip() for tag in item.split('、') if tag.strip())
        return tags

    def _load_known_tags(self) -> Tuple[Set[str], Set[str]]:
        """从 '收录曲目.xlsx' 加载已知列表。"""
        data_handler = DataHandler(self.config_handler) 
        try:
            df = data_handler._read_excel(Path("收录曲目.xlsx"), usecols_key='record')
            synthesizers = self._extract_tags(df['synthesizer']) if 'synthesizer' in df.columns else set()
            vocals = self._extract_tags(df['vocal']) if 'vocal' in df.columns else set()
            return synthesizers, vocals
        except FileNotFoundError:
            logger.error("错误：收录曲目文件 '收录曲目.xlsx' 不存在。")
            raise
        except Exception as e:
            logger.error(f"加载已知标签时出错: {e}")
            raise

    def _load_prompt_template(self, synthesizers: Set[str], vocals: Set[str]) -> str:
        """从文件加载提示词模板，并用已知标签列表格式化。"""
        try:
            with open("config/prompt_template.txt", "r", encoding="utf-8") as f:
                template = f.read()
            return template.format(
                known_synthesizers="\n".join(sorted(synthesizers)) or "无",
                known_vocals="\n".join(sorted(vocals)) or "无"
            )
        except (FileNotFoundError, KeyError) as e:
            logger.error(f"加载或格式化Prompt模板时出错: {e}")
            raise

    def _parse_json_from_response(self, text: str) -> Optional[dict]:
        """从AI响应中提取JSON。"""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))
                except json.JSONDecodeError:
                    pass
        logger.error("无法从AI响应中解析有效JSON。")
        return None

    async def _get_ai_tags_batch(self, batch_data: List[Dict]) -> Optional[List[Dict]]:
        """为一批视频调用AI API获取标注结果。"""
        user_content = json.dumps({"videos": batch_data}, ensure_ascii=False) 
        last_exception = None
        for attempt in range(self.max_attempts):
            try:
                async with self.semaphore:
                    params = {
                        "model": self.model_name, "messages": [{"role": "system", "content": self.prompt_template}, {"role": "user", "content": user_content}],
                        "temperature": 0.2,
                    }
                    if self.provider == 'API':
                        params["response_format"] = {"type": "json_object"}

                    response = await self.client.chat.completions.create(**params)
                
                message_content = response.choices[0].message.content
                if message_content:
                    logger.info(f"AI Raw Response (Attempt {attempt + 1}):\n---\n{message_content}\n---")
                else:
                    logger.warning(f"Attempt {attempt + 1}/{self.max_attempts}: AI did not return any message content.")
                    continue

                parsed_json = self._parse_json_from_response(message_content)
                if not parsed_json:
                    logger.warning(f"Attempt {attempt + 1}/{self.max_attempts}: Failed to parse JSON from response.")
                    continue 

                results = parsed_json.get("results", [])
                if len(results) == len(batch_data):
                    return results
                else:
                    logger.warning(f"Attempt {attempt + 1}/{self.max_attempts}: Result count mismatch.")
                    continue 

            except Exception as e:
                last_exception = e
                logger.error(f"Attempt {attempt + 1}/{self.max_attempts} failed with exception: {e}")
 
        logger.error(f"Batch failed after {self.max_attempts} attempts. Last exception: {last_exception}")
        return None
    
    def _prepare_dataframe(self) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[int, str]]:
        """读取并准备DataFrame，区分已标注和待处理的数据。"""
        df = pd.read_excel(self.input_file)
        tagging_cols = ['synthesizer', 'vocal', 'type']
        for col in tagging_cols:
            if col not in df.columns:
                df[col] = pd.NA
        
        for col in tagging_cols:
            df[col] = df[col].astype(object)
        df['status'] = 'check' 
        row_styles = {}
        tagged_mask = df[tagging_cols].notna().all(axis=1)

        done_indices = df[tagged_mask].index
        df.loc[done_indices, 'status'] = 'done'
        for idx in done_indices:
            row_styles[idx] = COLOR_LIGHT_BLUE      
        df_to_process = df[~tagged_mask].copy()
        logger.info(f"总计 {len(df)} 首。已标注 {len(done_indices)} 首，待处理 {len(df_to_process)} 首。")
        return df, df_to_process, row_styles

    def _apply_batch_results(self, df: pd.DataFrame, chunk_indices: pd.Index, results: List[Dict], styles: Dict[int, str]):
        """将一个批次的AI处理结果应用到主DataFrame上，并更新样式和status。"""
        tagging_cols = ['synthesizer', 'vocal', 'type']
        for idx, res in zip(chunk_indices, results):
            if not isinstance(res, dict):
                logger.warning(f"在索引 {idx} 处的结果不是一个有效的字典: {res}")
                df.at[idx, 'status'] = 'check' 
                continue
                
            if res.get('include'):
                tags = res.get('tags', {})
                for key, value in tags.items():
                    if key in df.columns:
                        df.at[idx, key] = value
                styles[idx] = COLOR_YELLOW 
                df.at[idx, 'status'] = 'auto' 
            else:
                for col in tagging_cols:
                    df.at[idx, col] = pd.NA 
                df.at[idx, 'status'] = 'check'

    async def _process_chunk(self, chunk: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[List[Dict]]]:
        """执行API调用并返回原始chunk及其结果。"""
        cols_to_send = ['title', 'uploader', 'intro', 'copyright']
        batch_records = chunk.reindex(columns=cols_to_send).to_dict('records')
        results = await self._get_ai_tags_batch(batch_records)
        return chunk, results

    async def run(self):
        """执行完整的AI标注流程，并实时保存进度。"""
        try:
            df, to_process, styles = self._prepare_dataframe()
        except FileNotFoundError:
            logger.error(f"输入文件不存在: {self.input_file}")
            return

        if to_process.empty:
            logger.info("没有需要AI处理的歌曲。")
            if not self.output_file.exists():
                save_to_excel(df, self.output_file, row_styles=styles)
            return

        save_to_excel(df, self.output_file, row_styles=styles)

        chunks = [to_process.iloc[i:i + self.batch_size] for i in range(0, len(to_process), self.batch_size)]
        total = len(chunks)
        logger.info(f"准备开始AI处理 {len(to_process)} 首歌曲，共分为 {total} 个批次...")

        tasks = [self._process_chunk(chunk) for chunk in chunks]
        
        completed, failed = 0, 0

        for future in asyncio.as_completed(tasks):
            processed_count = completed + failed + 1
            try:
                chunk, results = await future
                if results:
                    self._apply_batch_results(df, chunk.index, results, styles)
                    completed += 1
                    logger.info(f"批次 {processed_count}/{total} 成功.")
                else:
                    failed += 1
                    logger.warning(f"批次 {processed_count}/{total} 失败 (API返回空或格式错误).")
            except Exception as e:
                failed += 1
                logger.error(f"批次 {processed_count}/{total} 失败 (异常: {e}).")
            save_to_excel(df, self.output_file, row_styles=styles)