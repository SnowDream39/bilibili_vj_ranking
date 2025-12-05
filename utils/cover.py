# utils/cover.py
from pathlib import Path
from typing import List, Optional, Dict
from io import BytesIO
import random
import pandas as pd
import requests
from PIL import Image, ImageDraw, ImageFont

class Cover:
    def __init__(
        self,
        *,
        videos_root: Path,
        font_regular: str,
        font_bold: str,
        card_width: int,
        card_height: int,
        card_radius: int
    ):
        self.videos_root = videos_root
        self.font_file = font_regular
        self.font_bold_file = font_bold
        self.card_w = card_width
        self.card_h = card_height
        self.card_radius = card_radius
        
        self.videos_root.mkdir(exist_ok=True)

    def select_cover_urls_3_4(self, combined_rows: List[pd.Series]) -> List[str]:
        if not combined_rows:
            return []
            
        rank1_row = None
        others_top_rows = []
        new_rows = []
        
        for r in combined_rows:
            is_new = bool(r.get("is_new", False))
            try:
                rank = int(r.get("rank", 999))
            except:
                rank = 999
            
            if is_new:
                new_rows.append(r)
            else:
                if rank == 1:
                    rank1_row = r
                elif 2 <= rank <= 10:
                    others_top_rows.append(r)
                    
        selected_rows = []
        
        if rank1_row is not None:
            selected_rows.append(rank1_row)
            
        if len(others_top_rows) >= 3:
            selected_rows.extend(random.sample(others_top_rows, 3))
        else:
            selected_rows.extend(others_top_rows)
            
        if len(new_rows) >= 2:
            selected_rows.extend(random.sample(new_rows, 2))
        else:
            selected_rows.extend(new_rows)
            
        urls = []
        for r in selected_rows:
            u = str(r.get("image_url", "")).strip()
            if u:
                urls.append(u)
                
        return urls

    def select_cover_urls_grid(self, combined_rows: List[pd.Series]) -> List[str]:
        if not combined_rows:
            return []
        
        best_index = 0
        for idx, row in enumerate(combined_rows):
            if not bool(row.get("is_new", False)) and row.get("rank", None) == 1:
                best_index = idx
                break
        
        new_indices = [idx for idx, row in enumerate(combined_rows) if bool(row.get("is_new", False))]

        new1_index = new_indices[0] if len(new_indices) > 0 else -1
        new2_index = new_indices[1] if len(new_indices) > 1 else -1
        
        fixed_indices = [i for i in [best_index, new1_index, new2_index] if i != -1]
        used_set = set(fixed_indices)
        
        remaining = [i for i in range(len(combined_rows)) if i not in used_set]
        needed = 6 - len(fixed_indices)
        
        bottom_indices = random.sample(remaining, min(len(remaining), needed))
        final_indices = fixed_indices + bottom_indices
        
        urls = [str(combined_rows[i].get("image_url", "")).strip() for i in final_indices]
        return [u for u in urls if u]

    def _get_pil_image(self, url: str, bvid: str) -> Image.Image:
        cover_cache = self.videos_root / bvid / "cover.jpg"
        if cover_cache.exists():
            try: return Image.open(cover_cache).convert("RGBA")
            except: pass

        if url and url.startswith("http"):
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    img = Image.open(BytesIO(response.content)).convert("RGBA")
                    if not cover_cache.parent.exists():
                        cover_cache.parent.mkdir(parents=True)
                    img.convert("RGB").save(cover_cache)
                    return img
            except: pass
        return Image.new("RGBA", (300, 200), (200, 200, 200, 255))

    def _wrap_text(self, text: str, font: ImageFont.FreeTypeFont, max_width: int) -> List[str]:
        lines = []
        curr = ""
        if not text: return lines
        for char in text:
            if font.getbbox(curr + char)[2] <= max_width:
                curr += char
            else:
                lines.append(curr)
                curr = char
        if curr: lines.append(curr)
        return lines

    def create_card(self, row: pd.Series) -> Image.Image:
        title = str(row.get("title", ""))
        bvid = str(row.get("bvid", ""))
        author = str(row.get("author", ""))
        pubdate = str(row.get("pubdate", ""))
        image_url = str(row.get("image_url", ""))
        crossed_val = int(row.get("10w_crossed", 0))
        achievement_text = f"{crossed_val * 10}万播放达成!!"

        img = Image.new("RGBA", (self.card_w, self.card_h), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        bg_color = (255, 255, 255, 255)
        draw.rounded_rectangle((0, 0, self.card_w, self.card_h), radius=self.card_radius, fill=bg_color)

        margin = 20
        cover_h = self.card_h - 2 * margin
        cover_w = int(cover_h * (16 / 9))

        cover_img = self._get_pil_image(image_url, bvid)
        cover_img = cover_img.resize((cover_w, cover_h), Image.Resampling.LANCZOS)

        mask = Image.new("L", (cover_w, cover_h), 0)
        ImageDraw.Draw(mask).rounded_rectangle((0, 0, cover_w, cover_h), radius=15, fill=255)
        img.paste(cover_img, (margin, margin), mask)

        text_x = margin + cover_w + 30
        text_y = margin + 5
        text_width = self.card_w - text_x - margin

        f_title = ImageFont.truetype(self.font_bold_file, 34)
        f_info = ImageFont.truetype(self.font_file, 22)
        f_author = ImageFont.truetype(self.font_bold_file, 28)
        f_achieve = ImageFont.truetype(self.font_bold_file, 54)

        lines = self._wrap_text(title, f_title, text_width)
        for line in lines[:2]:
            draw.text((text_x, text_y), line, font=f_title, fill=(20, 20, 20))
            text_y += 45

        text_y += 5
        draw.text((text_x, text_y), f"{bvid}  {pubdate}", font=f_info, fill=(100, 100, 100))
        text_y += 32
        draw.text((text_x, text_y), f"作者：{author}", font=f_author, fill=(60, 60, 60))

        achieve_y = self.card_h - margin - 60
        draw.text((text_x + 2, achieve_y + 2), achievement_text, font=f_achieve, fill="#CCAC00")
        draw.text((text_x, achieve_y), achievement_text, font=f_achieve, fill="#FFD700")

        return img

    def create_header(self, width: int, height: int, opacity: float, ed_info: Optional[Dict] = None, list_top_y: Optional[int] = None) -> Image.Image:
        img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        
        alpha = int(255 * opacity)
        if alpha <= 0: return img

        title_font = ImageFont.truetype(self.font_bold_file, 80)
        title = "今日成就达成"
        bbox = title_font.getbbox(title)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        tx, ty = (width - tw) // 2, 150

        draw.text((tx + 3, ty + 3), title, font=title_font, fill=(255, 255, 255, alpha))
        draw.text((tx, ty), title, font=title_font, fill=(0, 139, 139, alpha))

        if ed_info and (ed_info.get("name") or ed_info.get("bvid")):
            line1 = f"ED：{ed_info.get('name', '')}"
            if ed_info.get("author"): line1 += f" / {ed_info['author']}"
            line2 = ed_info.get("bvid", "")

            ed_font = ImageFont.truetype(self.font_file, 32)
            
            region_top = ty + th + 25
            region_bottom = list_top_y if list_top_y is not None else int(height * 0.5)
            region_bottom = max(0, min(height, region_bottom))
            
            block_h = 80 # approx
            if region_bottom - region_top >= block_h + 10:
                ed_y = region_top + (region_bottom - region_top - block_h) // 2
            else:
                ed_y = region_top

            # 计算宽度用于右对齐
            l1_box = ed_font.getbbox(line1)
            l2_box = ed_font.getbbox(line2)
            w_blk = max(l1_box[2], l2_box[2])
            ed_x = width - 80 - w_blk

            draw.text((ed_x + 2, ed_y + 2), line1, font=ed_font, fill=(255, 255, 255, alpha))
            draw.text((ed_x, ed_y), line1, font=ed_font, fill=(0, 0, 0, alpha))
            
            y2 = ed_y + 40
            draw.text((ed_x + 2, y2 + 2), line2, font=ed_font, fill=(255, 255, 255, alpha))
            draw.text((ed_x, y2), line2, font=ed_font, fill=(0, 0, 0, alpha))

        return img
