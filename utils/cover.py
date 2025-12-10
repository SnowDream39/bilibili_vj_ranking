# utils/cover.py
from pathlib import Path
from typing import List, Optional, Dict
from io import BytesIO
import random
import subprocess
from datetime import datetime
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
        self.ffmpeg_bin = ffmpeg_bin
        
        self.videos_root.mkdir(exist_ok=True)
        
        # 星期主题色映射 (周一=0, 周日=6)
        self.weekday_colors = {
            0: "#8C4E70",  # 周一
            1: "#D66547",  # 周二
            2: "#595959",  # 周三
            3: "#4992A7",  # 周四
            4: "#BDBDBD",  # 周五
            5: "#C48700",  # 周六
            6: "#55CCCC",  # 周日
        }

    def _get_theme_color(self, issue_date: str) -> str:
        """根据期刊日期（YYYYMMDD）返回当天的主题色"""
        date_obj = datetime.strptime(issue_date, "%Y%m%d")
        weekday = date_obj.weekday()  
        return self.weekday_colors.get(weekday, "#55CCCC")

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

    def generate_grid_cover(
        self, 
        urls: List[str], 
        output_path: Path, 
        issue_date: str = "", 
        issue_index: int = 0
    ):
        """生成 16:9 风格封面"""
        if not urls:
            logger.warning("封面生成失败：没有可用的封面 URL")
            return

        display_urls = urls[:6]
        while len(display_urls) < 6:
            display_urls.append(display_urls[-1])

        theme_color = self._get_theme_color(issue_date)
        font_path = ffmpeg_escape_path(self.font_bold_file)

        cmd = [self.ffmpeg_bin, "-y"]
        for url in display_urls:
            cmd += ["-i", url]

        filters = []
        
        #图片处理
        filters.append(
            f"[0:v]scale=1000:562:force_original_aspect_ratio=increase,crop=1000:562,setsar=1,"
            f"pad=1024:586:12:12:white[v0_raw]"
        )
        filters.append(
            f"[1:v]scale=800:450:force_original_aspect_ratio=increase,crop=800:450,setsar=1,"
            f"pad=824:474:12:12:white[v1_raw]"
        )
        for i in range(2, 6):
            filters.append(
                f"[{i}:v]scale=420:236:force_original_aspect_ratio=increase,crop=420:236,setsar=1,"
                f"pad=436:252:8:8:white[v{i}_raw]"
            )

        #坐标
        pos = {
            # 小图层
            "v2": {"x": 68,   "y": 290},
            "v3": {"x": 524,  "y": 290},
            "v4": {"x": 980,  "y": 290},
            "v5": {"x": 1436, "y": 290},
            
            # 大图层
            "v0": {"x": 80,   "y": 460},
            "v1": {"x": 1016, "y": 570}, 
        }

        filters.append(
            f"[0:v]scale=1920:1080:force_original_aspect_ratio=increase,crop=1920:1080,"
            f"gblur=sigma=30,"
            f"eq=saturation=1.4:brightness=-0.05,"
            f"drawbox=c=white@0.1:t=fill[bg_blur]"
        )
        
        shadow_cmds = []
        shadow_cmds.append(
            f"drawbox=x={pos['v0']['x']+12}:y={pos['v0']['y']+12}:w=1024:h=586:c=black@0.2:t=fill"
        )
        shadow_cmds.append(
            f"drawbox=x={pos['v1']['x']+12}:y={pos['v1']['y']+12}:w=824:h=474:c=black@0.2:t=fill"
        )
        for k in ["v2", "v3", "v4", "v5"]:
            shadow_cmds.append(
                f"drawbox=x={pos[k]['x']+10}:y={pos[k]['y']+10}:w=436:h=252:c=black@0.25:t=fill"
            )
        
        shadow_str = ",".join(shadow_cmds) + ",gblur=sigma=25"
        filters.append(f"[bg_blur]{shadow_str}[bg_shadow]")

        current = "bg_shadow"
        for i, k in enumerate(["v2", "v3", "v4", "v5"]):
            nxt = f"l_s_{i}"
            src_label = f"{k}_raw"
            filters.append(f"[{current}][{src_label}]overlay=x={pos[k]['x']}:y={pos[k]['y']}[{nxt}]")
            current = nxt
            
        filters.append(f"[{current}][v0_raw]overlay=x={pos['v0']['x']}:y={pos['v0']['y']}[l_main]")
        filters.append(f"[l_main][v1_raw]overlay=x={pos['v1']['x']}:y={pos['v1']['y']}[l_final_img]")
        filters.append(
            f"[l_final_img]drawbox=x=0:y=0:w=1920:h=260:color={theme_color}:t=fill[banner]"
        )
        
        # 解析日期
        if issue_date:
            try:
                date_obj = datetime.strptime(issue_date, "%Y%m%d")
                month = date_obj.strftime("%m")
                day = date_obj.strftime("%d")
            except:
                month, day = "12", "09"
        else:
            month, day = "12", "09"

        # 分割线
        filters.append(
            f"[banner]drawbox=x=770:y=50:w=6:h=180:color=white@0.7:t=fill[deco]"
        )

        # 日期
        draw_month = (
            f"drawtext=fontfile='{font_path}':text='{month}/':"
            f"fontsize=140:fontcolor=white:"
            f"x=240:y=85:shadowx=4:shadowy=4:shadowcolor=black@0.3"
        )
        draw_day = (
            f"drawtext=fontfile='{font_path}':text='{day}':"
            f"fontsize=240:fontcolor=white:"
            f"x=460:y=60:shadowx=6:shadowy=6:shadowcolor=black@0.3"
        )
        
        # 标题
        draw_title_main = (
            f"drawtext=fontfile='{font_path}':text='虚拟歌手日刊':"
            f"fontsize=80:fontcolor=white@0.95:"
            f"x=800:y=50:shadowx=3:shadowy=3:shadowcolor=black@0.3"
        )
        draw_title_sub = (
            f"drawtext=fontfile='{font_path}':text='外语排行榜':"
            f"fontsize=100:fontcolor=white:"
            f"x=800:y=135:shadowx=4:shadowy=4:shadowcolor=black@0.3"
        )
        
        # 期数
        issue_text = f"VOL.{issue_index}" if issue_index > 0 else ""
        draw_issue = (
            f"drawtext=fontfile='{font_path}':text='{issue_text}':"
            f"fontsize=110:fontcolor=white@0.25:"
            f"x=1920-tw-140:y=(260-th)/2" 
        )
        
        filters.append(f"[deco]{draw_month}[t1]")
        filters.append(f"[t1]{draw_day}[t2]")
        filters.append(f"[t2]{draw_title_main}[t3]")
        filters.append(f"[t3]{draw_title_sub}[t4]")
        filters.append(f"[t4]{draw_issue}[vout]")

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

    def generate_vertical_cover(
        self, 
        urls: List[str], 
        output_path: Path, 
        issue_date: str = "", 
        issue_index: int = 0
    ):
        """生成 3:4 竖屏封面"""
        if not urls:
            logger.warning("3:4 封面生成失败：没有可用的封面 URL")
            return
        
        border_color = self._get_theme_color(issue_date)
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
        
        hero_y = "(H-h)/2 + 250"
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
        filters.append(f"[txt1]{draw_t2}[txt2]")

        if issue_date:
            try:
                date_obj = datetime.strptime(issue_date, "%Y%m%d")
                month = date_obj.strftime("%m")
                day = date_obj.strftime("%d")
            except:
                month = issue_date[-4:-2]
                day = issue_date[-2:]
            
            month_font_size = 300
            day_font_size = int(month_font_size * 2)  
            
            date_base_y = 1700
            
            draw_month = (
                f"drawtext=fontfile='{font_path}':text='{month}':"
                f"fontsize={month_font_size}:fontcolor={fill_color}:"
                f"borderw=24:bordercolor={border_color}:"
                f"x=(w/2)-500:y={date_base_y}:"
                f"shadowx=8:shadowy=8:shadowcolor=black@0.6"
            )
            
            draw_slash = (
                f"drawtext=fontfile='{font_path}':text='/':"
                f"fontsize={month_font_size}:fontcolor={fill_color}:"
                f"borderw=24:bordercolor={border_color}:"
                f"x=(w/2)-80:y={date_base_y}:"
                f"shadowx=6:shadowy=6:shadowcolor=black@0.5"
            )
            
            day_offset_y = (day_font_size - month_font_size) / 2
            draw_day = (
                f"drawtext=fontfile='{font_path}':text='{day}':"
                f"fontsize={day_font_size}:fontcolor={fill_color}:"
                f"borderw=32:bordercolor={border_color}:"
                f"x=(w/2)+80:y={date_base_y - day_offset_y}:"
                f"shadowx=10:shadowy=10:shadowcolor=black@0.6"
            )
            
            filters.append(f"[txt2]{draw_month}[txt3]")
            filters.append(f"[txt3]{draw_slash}[txt4]")
            filters.append(f"[txt4]{draw_day}[vout]")
        else:
            filters.append("[txt2]copy[vout]")

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
            
            block_h = 80
            if region_bottom - region_top >= block_h + 10:
                ed_y = region_top + (region_bottom - region_top - block_h) // 2
            else:
                ed_y = region_top

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
