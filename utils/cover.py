# utils/cover.py
from pathlib import Path
from typing import List, Optional, Dict
from io import BytesIO
import random
import subprocess
import pandas as pd
import requests
from PIL import Image, ImageDraw, ImageFont
from utils.logger import logger

def ffmpeg_escape_path(path: str) -> str:
    """FFmpeg 路径转义辅助函数"""
    p = str(path).replace("\\", "/")
    if ":" in p:
        drive, rest = p.split(":", 1)
        p = f"{drive}\\:{rest}"
    return p

class Cover:
    def __init__(
        self,
        *,
        videos_root: Path,
        font_regular: str,
        font_bold: str,
        card_width: int,
        card_height: int,
        card_radius: int,
        ffmpeg_bin: str = "ffmpeg" 
    ):
        self.videos_root = videos_root
        self.font_file = font_regular
        self.font_bold_file = font_bold
        self.card_w = card_width
        self.card_h = card_height
        self.card_radius = card_radius
        self.ffmpeg_bin = ffmpeg_bin # 保存 ffmpeg 路径
        
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

    def generate_grid_cover(self, urls: List[str], output_path: Path):
        """生成 16:9 网格封面"""
        if not urls:
            logger.warning("封面生成失败：没有可用的封面 URL")
            return

        cmd = [self.ffmpeg_bin, "-y"]
        for url in urls:
            cmd += ["-i", url]

        def tf(stream_index: int, width: int, height: int, label: str) -> str:
            return (
                f"[{stream_index}:v]scale={width}:{height}:force_original_aspect_ratio=increase,"
                f"crop={width}:{height},setsar=1,"
                f"drawbox=t=6:c=white[{label}]"
            )

        filters = [
            tf(0, 1280, 720, "v0"),
            tf(1, 640, 360, "v1"),
            tf(2, 640, 360, "v2"),
            tf(3, 640, 360, "v3"),
            tf(4, 640, 360, "v4"),
            tf(5, 640, 360, "v5"),
            "[v0][v1][v2][v3][v4][v5]xstack=inputs=6:layout="
            "0_0|1280_0|1280_360|0_720|640_720|1280_720[bg]",
        ]
        
        font_path = ffmpeg_escape_path(self.font_bold_file)
        
        text1 = "虚拟歌手日刊"
        text2 = "外语排行榜"
        fill_color = "white@0.95"
        border_color = "#55CCCC"
        border_w = 12
        font_size_1 = 160
        font_size_2 = 110
        
        font_obj = ImageFont.truetype(self.font_bold_file, font_size_1)
        w1 = font_obj.getlength(text1)


        right_anchor_x = (1920 / 2) + (w1 / 2)
        base_y = 550
        
        draw_t1 = (
            f"drawtext=fontfile='{font_path}':text='{text1}':"
            f"fontsize={font_size_1}:fontcolor={fill_color}:"
            f"borderw={border_w}:bordercolor={border_color}:"
            f"x={right_anchor_x}-tw:y={base_y}:"
            f"shadowx=5:shadowy=5:shadowcolor=black@0.5"
        )
        
        gap = 20
        draw_t2 = (
            f"drawtext=fontfile='{font_path}':text='{text2}':"
            f"fontsize={font_size_2}:fontcolor={fill_color}:"
            f"borderw={border_w}:bordercolor={border_color}:"
            f"x={right_anchor_x}-tw:y={base_y + font_size_1 + gap}:"
            f"shadowx=3:shadowy=3:shadowcolor=black@0.5"
        )
        
        filters.append(f"[bg]{draw_t1}[tmp1]")
        filters.append(f"[tmp1]{draw_t2}[vout]")

        cmd += [
            "-filter_complex", ";".join(filters),
            "-map", "[vout]",
            "-frames:v", "1",
            "-q:v", "2",
            str(output_path),
            "-loglevel", "error",
        ]

        try:
            subprocess.run(cmd, check=True)
            logger.info(f"封面图片已保存: {output_path}")
        except subprocess.CalledProcessError as e:
            logger.error(f"封面图片生成失败: {e}")

    def generate_vertical_cover(self, urls: List[str], output_path: Path):
        """生成 3:4 竖屏封面"""
        if not urls:
            logger.warning("3:4 封面生成失败：没有可用的封面 URL")
            return
        valid_urls = urls[:6]
        count = len(valid_urls)

        cmd = [self.ffmpeg_bin, "-y"]
        for u in valid_urls:
            cmd += ["-i", u]

        W, H = 1920, 2560
        font_path = ffmpeg_escape_path(self.font_bold_file)

        bg_filter = (
            f"[0:v]scale={W}:{H}:force_original_aspect_ratio=increase,"
            f"crop={W}:{H},setsar=1,"
            f"boxblur=40:5,eq=brightness=-0.1:saturation=1.3[bg]"
        )
        filters = [bg_filter]
        
        hero_w = 1600
        hero_h = int(hero_w * 9 / 16)
        hero_pad = 20
        small_w = 1000
        small_h = int(small_w * 9 / 16)
        small_pad = 15
        
        processed_labels = []
        for i in range(count):
            is_hero = (i == 0)
            tw = hero_w if is_hero else small_w
            th = hero_h if is_hero else small_h
            pad = hero_pad if is_hero else small_pad
            pw, ph = tw + 2 * pad, th + 2 * pad
            lbl = f"img{i}"
            processed_labels.append(lbl)
            filters.append(
                f"[{i}:v]scale={tw}:{th}:force_original_aspect_ratio=decrease,"
                f"pad={pw}:{ph}:{pad}:{pad}:white[{lbl}]"
            )
        
        hero_y = "(H-h)/2 + 350"
        hero_x = "(W-w)/2"
        top_row_y = 600
        btm_row_y = "H-h-100"
        
        small_positions = [
            {"x": "-100", "y": top_row_y}, 
            {"x": "W-w+100", "y": top_row_y},
            {"x": "-150", "y": btm_row_y},
            {"x": "(W-w)/2", "y": btm_row_y},
            {"x": "W-w+150", "y": btm_row_y},
        ]
        
        current_bg = "bg"
        for i in range(1, count):
            lbl = processed_labels[i]
            pos_idx = i - 1
            pos = small_positions[pos_idx] if pos_idx < len(small_positions) else small_positions[-1]
            next_bg = f"tmp_bg_{i}"
            filters.append(f"[{current_bg}][{lbl}]overlay=x={pos['x']}:y={pos['y']}[{next_bg}]")
            current_bg = next_bg
            
        hero_lbl = processed_labels[0]
        filters.append(
            f"[{hero_lbl}]split[h_src][h_sh_raw];"
            f"[h_sh_raw]drawbox=c=black:t=fill,format=rgba,gblur=sigma=40,colorchannelmixer=aa=0.45[hero_shadow]"
        )
        filters.append(f"[{current_bg}][hero_shadow]overlay=x={hero_x}+30:y={hero_y}+40[bg_w_shadow]")
        filters.append(f"[bg_w_shadow][h_src]overlay=x={hero_x}:y={hero_y}[combined_img]")
        
        text1 = "虚拟歌手日刊"
        text2 = "外语排行榜"
        fill_color = "white@0.95"
        border_color = "#55CCCC"
        border_w = 22
        font_size_1 = 260
        font_size_2 = 220
        title_base_y = 220

        font_obj = ImageFont.truetype(self.font_bold_file, font_size_1)
        w1 = font_obj.getlength(text1)
            
        right_anchor_x = (W / 2) + (w1 / 2)
        
        draw_t1 = (
            f"drawtext=fontfile='{font_path}':text='{text1}':fontsize={font_size_1}:"
            f"fontcolor={fill_color}:borderw={border_w}:bordercolor={border_color}:"
            f"x={right_anchor_x}-tw:y={title_base_y}:shadowx=8:shadowy=8:shadowcolor=black@0.4"
        )
        gap = 40
        draw_t2 = (
            f"drawtext=fontfile='{font_path}':text='{text2}':fontsize={font_size_2}:"
            f"fontcolor={fill_color}:borderw={border_w}:bordercolor={border_color}:"
            f"x={right_anchor_x}-tw:y={title_base_y + font_size_1 + gap}:"
            f"shadowx=5:shadowy=5:shadowcolor=black@0.4"
        )

        filters.append(f"[combined_img]{draw_t1}[txt1]")
        filters.append(f"[txt1]{draw_t2}[vout]")

        cmd += [
            "-filter_complex", ";".join(filters),
            "-map", "[vout]",
            "-frames:v", "1",
            "-q:v", "2",
            str(output_path),
            "-loglevel", "error",
        ]

        try:
            subprocess.run(cmd, check=True)
            logger.info(f"3:4 封面图片已保存: {output_path}")
        except subprocess.CalledProcessError as e:
            logger.error(f"3:4 封面图片生成失败: {e}")

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
