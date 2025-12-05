# utils/clip_overlay.py

from pathlib import Path
from typing import Dict, List, Tuple
import math
from PIL import ImageFont

import pandas as pd

def ffmpeg_escape(text: str) -> str:
    s = str(text)
    return (s.replace("\\", "\\\\")
             .replace(":", r"\:")
             .replace("'", r"\'")
             .replace("%", r"\%"))

def ffmpeg_escape_path(path: str) -> str:
    p = path.replace("\\", "/")
    if ":" in p:
        drive, rest = p.split(":", 1)
        p = f"{drive}\\:{rest}"
    return p

def format_number(x) -> str:
    if x is None:
        return "-"
    try:
        if isinstance(x, float) and math.isnan(x):
            return "-"
        return f"{int(x):,}"
    except Exception:
        if str(x).strip() == "":
            return "-"
        return str(x)

def split_text_by_pixel_width(
    text: str, 
    font_path: str, 
    font_size: int, 
    max_width: int, 
    max_lines: int = 3
) -> List[str]:
    """
    使用 PIL 精确计算文字像素宽度进行换行。
    """
    text = (text or "").strip()
    if not text:
        return []

    font = ImageFont.truetype(font_path, font_size)
    lines = []
    current_line = ""
    for char in text:
        test_line = current_line + char
        width = font.getlength(test_line)

        if width <= max_width:
            current_line = test_line
        else:
            if len(lines) >= max_lines - 1:
                while current_line and font.getlength(current_line + "...") > max_width:
                    current_line = current_line[:-1]
                lines.append(current_line + "...")
                return lines
            
            lines.append(current_line)
            current_line = char
    
    if current_line:
        lines.append(current_line)
        
    return lines


def build_clip_overlay_cmd(
    *,
    segment_source_path: Path,
    row: pd.Series,
    clip_index: int,
    issue_date_str: str,
    daily_video_dir: Path,
    icon_dir: Path,
    font_file: str,
) -> Tuple[List[str], Path]:

    bvid = str(row.get("bvid", "")).strip()
    title = str(row.get("title", "")).strip()
    author = str(row.get("author", "")).strip()
    pubdate = str(row.get("pubdate", "")).strip()
    point = row.get("point", "")
    view = row.get("view", "")
    favorite = row.get("favorite", "")
    coin = row.get("coin", "")
    like = row.get("like", "")
    danmaku = row.get("danmaku", "")
    reply = row.get("reply", "")
    share = row.get("share", "")
    rank = row.get("rank", None)
    count = row.get("count", 0)
    is_new = bool(row.get("is_new", False))

    fontfile_expr = ffmpeg_escape_path(font_file)
    bvid_text = ffmpeg_escape(bvid)
    pubdate_text = ffmpeg_escape(pubdate)
    author_text = ffmpeg_escape(author)
    count_text = ffmpeg_escape(count)
    point_text = ffmpeg_escape(format_number(point))
    wm_line1 = ffmpeg_escape("术力口数据姬")
    wm_line2 = ffmpeg_escape("vocabili.top")

    TITLE_START_Y = 60
    TITLE_FONT_SIZE = 50
    TITLE_LINE_H = 80
    MAX_TEXT_WIDTH = 960 

    title_lines = split_text_by_pixel_width(
        title, 
        font_file, 
        TITLE_FONT_SIZE, 
        MAX_TEXT_WIDTH, 
        max_lines=3
    )
    if not title_lines:
        title_lines = [""]
    
    num_lines = len(title_lines)
    shift_y = (num_lines - 1) * TITLE_LINE_H

    left_text_filters = []
    curr_title_y = TITLE_START_Y
    for line in title_lines:
        esc_line = ffmpeg_escape(line)
        f = (
            f"drawtext=fontfile='{fontfile_expr}':text='{esc_line}':x=60:y={curr_title_y}:"
            f"fontsize={TITLE_FONT_SIZE}:fontcolor=white:box=1:boxcolor=black@0.45:boxborderw=14:"
            f"shadowx=2:shadowy=2:shadowcolor=black@0.55"
        )
        left_text_filters.append(f)
        curr_title_y += TITLE_LINE_H

    RANK_Y = 140 + shift_y
    ARROW_Y = 150 + shift_y
    
    rank_before_raw = row.get("rank_before", None)
    rank_before_str = str(rank_before_raw).strip() if rank_before_raw is not None else "-"

    if is_new:
        rank_text = ffmpeg_escape("NEW!!")
        rank_color = "#FF3333"
        show_rank_change = False
        arrow_text_escaped = ""
        prev_rank_text_escaped = ""
    else:
        rank_text = ffmpeg_escape(f"# {rank}")
        if rank == 1:
            rank_color = "#FFD700"
        elif rank == 2:
            rank_color = "#C0C0C0"
        elif rank == 3:
            rank_color = "#CD7F32"
        else:
            rank_color = "#00E5FF"

        show_rank_change = True

        if rank_before_str == "-":
            arrow_text = "▲"
            arrow_color = "#FF3333"
            prev_rank_text = "NEW"
        else:
            rank_before = int(rank_before_str)
            prev_rank_text = str(rank_before)

            if rank_before > rank:
                arrow_text = "▲"
                arrow_color = "#FF4444"
            elif rank_before < rank:
                arrow_text = "▼"
                arrow_color = "#4488FF"
            else:
                arrow_text = "■"
                arrow_color = "#888888"

        arrow_text_escaped = ffmpeg_escape(arrow_text)
        prev_rank_text_escaped = ffmpeg_escape(prev_rank_text)

    INFO_START_Y = 280 + shift_y
    
    left_text_filters += [
        (
            f"drawtext=fontfile='{fontfile_expr}':text='{rank_text}':x=60:y={RANK_Y}:"
            f"fontsize=120:fontcolor={rank_color}:"
            f"shadowx=3:shadowy=3:shadowcolor=black@0.9"
        ),
        (
            f"drawtext=fontfile='{fontfile_expr}':text='{bvid_text}':x=60:y={INFO_START_Y}:"
            f"fontsize=36:fontcolor=white:shadowx=2:shadowy=2:shadowcolor=black@0.9"
        ),
        (
            f"drawtext=fontfile='{fontfile_expr}':text='{pubdate_text}':x=60:y={INFO_START_Y + 40}:"
            f"fontsize=36:fontcolor=white:shadowx=2:shadowy=2:shadowcolor=black@0.9"
        ),
        (
            f"drawtext=fontfile='{fontfile_expr}':text='作者：{author_text}':x=60:y={INFO_START_Y + 80}:"
            f"fontsize=36:fontcolor=white:shadowx=2:shadowy=2:shadowcolor=black@0.9"
        ),
        (
            f"drawtext=fontfile='{fontfile_expr}':text='上榜次数：{count_text}':x=60:y={INFO_START_Y + 120}:"
            f"fontsize=36:fontcolor=white:shadowx=2:shadowy=2:shadowcolor=black@0.9"
        ),
    ]

    if show_rank_change and arrow_text_escaped:
        rank = int(rank) if rank is not None else 0
        arrow_x = 310 if rank >= 10 else 240

        if prev_rank_text_escaped:
            prev_rank_int = int(prev_rank_text_escaped) if prev_rank_text_escaped.isdigit() else 0
            is_one_digit = prev_rank_int > 0 and prev_rank_int < 10
            is_new_text = prev_rank_text_escaped == "NEW"
            
            curr_arrow_y = ARROW_Y
            prev_rank_x_offset = 22
            prev_rank_y_offset = 22
            
            if arrow_text_escaped == ffmpeg_escape("■"):
                prev_rank_y_offset = 22
                if is_one_digit: prev_rank_x_offset += 10
            elif arrow_text_escaped == ffmpeg_escape("▲"):
                prev_rank_y_offset = 32
                if is_one_digit: prev_rank_x_offset += 10
            elif arrow_text_escaped == ffmpeg_escape("▼"):
                prev_rank_y_offset = 12
                if is_one_digit: prev_rank_x_offset += 10

            if is_new_text:
                prev_rank_x_offset = 0
                prev_rank_fontsize = 40
            else:
                prev_rank_fontsize = 40

            arrow_filter = (
                f"drawtext=fontfile='{fontfile_expr}':text='{arrow_text_escaped}':"
                f"x={arrow_x}:y={curr_arrow_y}:"
                f"fontsize=90:fontcolor={arrow_color}:"
                f"shadowx=2:shadowy=2:shadowcolor=black@0.8"
            )
            left_text_filters.append(arrow_filter)

            prev_rank_filter = (
                f"drawtext=fontfile='{fontfile_expr}':text='{prev_rank_text_escaped}':"
                f"x={arrow_x + prev_rank_x_offset}:y={curr_arrow_y + prev_rank_y_offset}:"
                f"fontsize={prev_rank_fontsize}:fontcolor=white:"
                f"shadowx=2:shadowy=2:shadowcolor=black@0.9"
            )
            left_text_filters.append(prev_rank_filter)

    POINT_Y = 140 + shift_y
    right_margin = 60
    point_value_filter = (
        f"drawtext=fontfile='{fontfile_expr}':text='{point_text}':"
        f"x=w-tw-{right_margin+60}:"
        f"y={POINT_Y}:"
        f"fontsize=100:"
        f"fontcolor=#FFD700:"
        f"shadowx=2:shadowy=2:shadowcolor=black@0.6"
    )
    point_unit_filter = (
        f"drawtext=fontfile='{fontfile_expr}':text='pts':"
        f"x=w-tw-{right_margin}:"
        f"y={POINT_Y}+(100-30)/2:"
        f"fontsize=30:"
        f"fontcolor=white"
    )

    icon_map = {
        "播放": icon_dir / "播放.png",
        "收藏": icon_dir / "收藏.png",
        "硬币": icon_dir / "硬币.png",
        "点赞": icon_dir / "点赞.png",
        "弹幕": icon_dir / "弹幕.png",
        "评论": icon_dir / "评论.png",
        "分享": icon_dir / "分享.png",
    }

    stats = [
        ("播放", view),
        ("收藏", favorite),
        ("硬币", coin),
        ("点赞", like),
        ("弹幕", danmaku),
        ("评论", reply),
        ("分享", share),
    ]

    icon_size = 30
    value_x_right = "w-tw-50"
    icon_x_right = "W-220"
    value_x_left = "w-tw-240"
    icon_x_left = "W-410"
    icon_x_center = "W-315"
    value_x_center = "w-tw-145"

    base_y = POINT_Y + 100
    line_height = 38

    cmd: List[str] = [
        "-y",
        "-i",
        str(segment_source_path),
    ]

    icon_input_indices: Dict[str, int] = {}
    for idx, (label, value) in enumerate(stats):
        icon_path = icon_map[label]
        cmd += ["-loop", "1", "-i", str(icon_path)]
        icon_input_indices[label] = idx + 1 

    base_filters = [
        "[0:v]settb=AVTB,setpts=PTS-STARTPTS,setsar=1,fps=60[v0]",
        "[v0]scale=trunc(1920*a/2)*2:1920,setsar=1,"
        "crop=1080:1920:(in_w-1080)/2:(in_h-1920)/2,boxblur=20:8[bg]",
        "[v0]scale=1080:-1,setsar=1[fg]",
        "[bg][fg]overlay=(W-w)/2:(H-h)/2[vbase]",
        "[0:a]asetpts=PTS-STARTPTS[aout]",
    ]

    icon_filters: List[str] = []
    icon_processed_labels: Dict[str, Tuple[str, str]] = {}

    for idx, (label, value) in enumerate(stats):
        input_idx = icon_input_indices[label]
        
        raw_scaled_label = f"icon{idx}_raw"
        shadow_label = f"icon{idx}_shadow"
        main_label = f"icon{idx}_main"
        
        icon_processed_labels[label] = (main_label, shadow_label)

        gray_val = 220 
        
        filter_chain = (
            f"[{input_idx}:v]"
            f"scale={icon_size}:{icon_size}:flags=lanczos,"
            f"format=rgba,"
            f"split[{raw_scaled_label}_1][{raw_scaled_label}_2];"
            f"[{raw_scaled_label}_1]geq=r=0:g=0:b=0:a='alpha(X,Y)*0.6',gblur=sigma=1[{shadow_label}];"
            f"[{raw_scaled_label}_2]geq=r={gray_val}:g={gray_val}:b={gray_val}:a='alpha(X,Y)'[{main_label}]"
        )
        icon_filters.append(filter_chain)

    stats_layout: List[Tuple[str, object, str, str, int]] = []

    y_play = base_y
    stats_layout.append(("播放", view, icon_x_center, value_x_center, y_play))

    others = [
        ("收藏", favorite),
        ("硬币", coin),
        ("点赞", like),
        ("弹幕", danmaku),
        ("评论", reply),
        ("分享", share),
    ]
    for idx, (label, value) in enumerate(others):
        row_idx = idx % 3
        col_idx = idx // 3  
        y = base_y + (row_idx + 1) * line_height

        if col_idx == 0:
            icon_x = icon_x_left
            value_x = value_x_left
        else:
            icon_x = icon_x_right
            value_x = value_x_right

        stats_layout.append((label, value, icon_x, value_x, y))

    stat_value_filters: List[str] = []
    for label, value, icon_x_pos, value_x_pos, y in stats_layout:
        value_escaped = ffmpeg_escape(format_number(value))
        stat_value_filter = (
            f"drawtext=fontfile='{fontfile_expr}':text='+{value_escaped}':"
            f"x={value_x_pos}:y={y+2}:"
            f"fontsize=30:fontcolor=#FFD700:"
            f"shadowx=1:shadowy=1:shadowcolor=black@0.5"
        )
        stat_value_filters.append(stat_value_filter)

    watermark_filters = [
        (
            f"drawtext=fontfile='{fontfile_expr}':text='{wm_line1}':"
            f"x=60:y=h-600:fontsize=36:fontcolor=white@0.65:"
            f"shadowx=1:shadowy=1:shadowcolor=black@0.65"
        ),
        (
            f"drawtext=fontfile='{fontfile_expr}':text='{wm_line2}':"
            f"x=60:y=h-560:fontsize=32:fontcolor=white@0.35:"
            f"shadowx=1:shadowy=1:shadowcolor=black@0.35"
        ),
    ]

    draw_ops = (
        left_text_filters
        + [point_value_filter, point_unit_filter]
        + stat_value_filters
        + watermark_filters
    )

    draw_filters: List[str] = []
    current_label = "vbase"

    for idx, expr in enumerate(draw_ops):
        next_label = f"vtxt{idx}"
        draw_filters.append(f"[{current_label}]{expr}[{next_label}]")
        current_label = next_label

    shadow_offset = 2
    
    for idx, (label, value, icon_x_pos, value_x_pos, y) in enumerate(stats_layout):
        main_label, shadow_label = icon_processed_labels[label]
        
        step1_label = f"vicon{idx}_sh"
        step2_label = f"vicon{idx}"
        
        shadow_overlay = (
            f"[{current_label}][{shadow_label}]"
            f"overlay={icon_x_pos}+{shadow_offset}:{y}+{shadow_offset}[{step1_label}]"
        )
        draw_filters.append(shadow_overlay)
        
        main_overlay = (
            f"[{step1_label}][{main_label}]"
            f"overlay={icon_x_pos}:{y}[{step2_label}]"
        )
        draw_filters.append(main_overlay)
        
        current_label = step2_label

    final_video_label = current_label

    filter_complex = ";".join(base_filters + icon_filters + draw_filters)

    daily_video_dir.mkdir(exist_ok=True)
    clip_filename = daily_video_dir / f"tmp_{issue_date_str}_{clip_index:02d}_{bvid}.mp4"

    cmd += [
        "-filter_complex",
        filter_complex,
        "-map",
        f"[{final_video_label}]",
        "-map",
        "[aout]",
        "-shortest",
    ]

    return cmd, clip_filename
