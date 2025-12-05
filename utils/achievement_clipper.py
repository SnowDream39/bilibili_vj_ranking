# utils/achievement_clipper.py
from pathlib import Path
from typing import List, Tuple, Dict
import pandas as pd
import yaml
from PIL import Image
from utils.logger import logger
from utils.cover import Cover

class AchievementClipper:
    def __init__(self, achievement_dir: Path, config_dir: Path, image_factory: Cover):
        self.achievement_dir = achievement_dir
        self.config_dir = config_dir
        self.img_factory = image_factory

    def load_rows(self, excel_date_str: str, issue_date_str: str) -> List[pd.Series]:
        filename = f"十万记录{excel_date_str}与{issue_date_str}.xlsx"
        path = self.achievement_dir / filename
        
        if not path.exists():
            logger.warning(f"Achievement excel not found: {path}")
            return []
            
        df = pd.read_excel(path)
        if df.empty: return []
        
        if "10w_crossed" in df.columns:
            df = df.sort_values("10w_crossed", ascending=False)
            
        rows = [row for _, row in df.iterrows()]
        logger.info(f"Achievement count: {len(rows)}")
        return rows

    def build_strip(self, rows: List[pd.Series], width: int, gap: int) -> Tuple[Image.Image, int]:
        if not rows:
            return Image.new("RGBA", (width, 100), (0,0,0,0)), 100
            
        # 使用注入的 image_factory 生成卡片，无需传参
        cards = [self.img_factory.create_card(row) for row in rows]
        
        card_h = cards[0].height
        card_w = cards[0].width
        total_h = len(cards) * (card_h + gap)
        
        strip = Image.new("RGBA", (width, total_h), (0,0,0,0))
        x_pos = (width - card_w) // 2
        
        for i, card in enumerate(cards):
            y = i * (card_h + gap)
            strip.paste(card, (x_pos, y), card)
            
        return strip, total_h

    def get_ed_info(self, issue_index: int) -> Dict[str, str]:
        yaml_path = self.config_dir / "ED.yaml"
        default = {"bvid": None, "name": "", "author": ""}
        
        if not yaml_path.exists():
            return default
            
        try:
            with open(yaml_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                raw = data.get(issue_index) or data.get(str(issue_index))
                
                if not raw: return default
                
                if isinstance(raw, str):
                    return {"bvid": raw.strip(), "name": "", "author": ""}
                elif isinstance(raw, dict):
                    return {
                        "bvid": str(raw.get("bvid", "")).strip(),
                        "name": str(raw.get("name", "")).strip(),
                        "author": str(raw.get("author", "")).strip(),
                    }
        except Exception as e:
            logger.error(f"Error reading ED yaml: {e}")
            
        return default
