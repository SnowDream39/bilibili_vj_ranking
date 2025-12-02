import re
import math
import random
import unicodedata
import subprocess
import warnings
import shutil
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import yaml
from PIL import Image, ImageDraw, ImageFont

from utils.climax_clipper import find_climax_segment

warnings.filterwarnings("ignore", category=UserWarning)

TOP_N = 10
TOTAL_DIR = Path("差异/合并表格")
NEW_SONG_DIR = Path("新曲榜")
ACHIEVEMENT_DIR = Path("整数播放达成/十万")
CONFIG_DIR = Path("config")

CLIP_DURATION = 10.0
VIDEOS_ROOT = Path("videos")
DAILY_VIDEO_DIR = Path("daily_video")
FFMPEG_BIN = "ffmpeg"
FIRST_ISSUE_DATE_STR = "20240703"

FONT_FILE = r"E:\Users\20330\Downloads\Noto_Sans_SC\static\NotoSansSC-Medium.ttf"
FONT_BOLD_FILE = r"E:\Users\20330\Downloads\Noto_Sans_SC\static\NotoSansSC-Bold.ttf"

SCROLL_BG_COLOR = (216, 236, 241)
CARD_W, CARD_H = 960, 280
CARD_RADIUS = 30
CARD_GAP = 50
SCROLL_HOLD_TIME = 5.0
SCROLL_SPEED_PPS = 150.0


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


def wrap_title(text: str, max_units_per_line: int = 50) -> str:
    text = (text or "").strip()
    if not text:
        return ""

    def char_units(ch: str) -> int:
        return 2 if unicodedata.east_asian_width(ch) in ("W", "F", "A") else 1

    units = 0
    out_chars = []
    for ch in text:
        unit = char_units(ch)
        if units + unit > max_units_per_line:
            break
        out_chars.append(ch)
        units += unit

    if len(out_chars) < len(text):
        return "".join(out_chars).rstrip() + "..."
    return "".join(out_chars)


def get_latest_excel(excel_dir: Path) -> Path:
    files = list(excel_dir.glob("*.xlsx"))
    if not files:
        raise FileNotFoundError(f"目录中没有找到 Excel 文件: {excel_dir}")
    latest = max(files, key=lambda p: p.stat().st_mtime)
    print(f"使用最新 Excel 文件: {latest}")
    return latest


def infer_issue_info(excel_path: Path) -> Tuple[str, int, str]:
    stem = excel_path.stem
    m = re.search(r"(20\d{6})", stem)
    excel_date_str = m.group(1) if m else FIRST_ISSUE_DATE_STR

    excel_dt = datetime.strptime(excel_date_str, "%Y%m%d")
    first_issue_dt = datetime.strptime(FIRST_ISSUE_DATE_STR, "%Y%m%d")
    issue_video_dt = excel_dt - timedelta(days=1)
    issue_date_str = issue_video_dt.strftime("%Y%m%d")
    diff_days = (issue_video_dt - first_issue_dt).days
    issue_index = max(1, diff_days + 1)
    print(f"期数日期: {issue_date_str}, 期数索引: {issue_index}")
    return issue_date_str, issue_index, excel_date_str


def get_newsong_excel_for(excel_path: Path, newsong_dir: Path) -> Path:
    files = list(newsong_dir.glob("*.xlsx"))
    m = re.search(r"(20\d{6})", excel_path.stem)
    date_str = m.group(1)
    candidates = [p for p in files if date_str in p.stem]
    return candidates[0]


def get_ed_info(issue_index: int) -> Optional[Dict[str, str]]:
    CONFIG_DIR.mkdir(exist_ok=True)
    yaml_path = CONFIG_DIR / "ED.yaml"
    if not yaml_path.exists():
        print(f"警告: 未找到 ED 配置文件: {yaml_path}")
        return None

    try:
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            if not isinstance(data, dict):
                return None

            raw = data.get(issue_index) or data.get(str(issue_index))
            if raw is None:
                print(f"ED 配置中没有找到期数 {issue_index} 对应信息: {yaml_path}")
                return None

            if isinstance(raw, str):
                info = {
                    "bvid": raw.strip(),
                    "name": "",
                    "author": "",
                }
            elif isinstance(raw, dict):
                info = {
                    "bvid": str(raw.get("bvid", "")).strip(),
                    "name": str(raw.get("name", "")).strip(),
                    "author": str(raw.get("author", "")).strip(),
                }
            else:
                return None

            if not info["bvid"]:
                print(f"期数 {issue_index} ED 信息缺少 bvid")
                return None

            print(
                f"期数 {issue_index} ED 信息: "
                f"bvid={info['bvid']} name={info['name']} author={info['author']}"
            )
            return info

    except Exception as e:
        print(f"读取 ED 配置出错: {e}")
        return None


def get_ed_bgm_bvid(issue_index: int) -> Optional[str]:
    info = get_ed_info(issue_index)
    return info["bvid"] if info else None


def ffmpeg_escape(text: str) -> str:
    s = str(text)
    s = s.replace("\\", "\\\\")
    s = s.replace(":", r"\:")
    s = s.replace("'", r"\'")
    s = s.replace("%", r"\%")
    return s


def ffmpeg_escape_path(path: str) -> str:
    p = path.replace("\\", "/")
    if ":" in p:
        drive, rest = p.split(":", 1)
        p = f"{drive}\\:{rest}"
    return p


def add_x264_encode_args(cmd: List[str]) -> None:
    cmd += [
        "-c:v",
        "libx264",
        "-crf",
        "16",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
    ]

def generate_cover_image_3_4_from_rows(combined_rows: List[pd.Series], cover_path: Path) -> None:
    """
    生成 3:4 封面：
    - 使用 3 张 16:9 封面（1920x1080）
    - 上：第二名 / 第二张
    - 中：第一名 / 第一张
    - 下：第三张
    - 最后整体裁剪成 1920x2560（3:4）
    """
    normal_rows = [r for r in combined_rows if not bool(r.get("is_new", False))]

    def rank_key(r: pd.Series):
        v = r.get("rank", None)
        try:
            return int(v)
        except Exception:
            return 999999

    normal_rows_sorted = sorted(normal_rows, key=rank_key)
    rows3: List[pd.Series] = normal_rows_sorted[:3]

    urls = [str(r.get("image_url", "")).strip() for r in rows3]
    urls = [u for u in urls if u]  # 过滤空 url

    while len(urls) < 3:
        urls.append(urls[-1])

    urls = urls[:3]

    cmd = [FFMPEG_BIN, "-y"]
    for u in urls:
        cmd += ["-i", u]

    out_w = 1920
    out_h = 2560

    filter_lines = [
        "[0:v]scale=1920:1080:force_original_aspect_ratio=increase,"
        "crop=1920:1080,setsar=1[v0]",
        "[1:v]scale=1920:1080:force_original_aspect_ratio=increase,"
        "crop=1920:1080,setsar=1[v1]",
        "[2:v]scale=1920:1080:force_original_aspect_ratio=increase,"
        "crop=1920:1080,setsar=1[v2]",
        "[v1][v0][v2]vstack=inputs=3[vstack]",
        f"[vstack]crop={out_w}:{out_h}:0:(in_h-{out_h})/2[vout]",
    ]

    cmd += [
        "-filter_complex",
        ";".join(filter_lines),
        "-map",
        "[vout]",
        "-frames:v",
        "1",
        "-q:v",
        "2",
        str(cover_path),
        "-loglevel",
        "error",
    ]

    try:
        subprocess.run(cmd, check=True)
        print(f"3:4 封面图片已保存: {cover_path}")
    except subprocess.CalledProcessError as e:
        print(f"3:4 封面生成失败: {e}")

def download_bilibili_video(bvid: str) -> Optional[Path]:
    import yt_dlp

    VIDEOS_ROOT.mkdir(exist_ok=True)
    bvid_dir = VIDEOS_ROOT / bvid
    bvid_dir.mkdir(exist_ok=True)

    cached_video = bvid_dir / f"{bvid}.mp4"
    if cached_video.exists():
        return cached_video

    print(f"开始下载视频: {bvid}")
    url = f"https://www.bilibili.com/video/{bvid}"
    out_template = bvid_dir / f"{bvid}.%(ext)s"

    ydl_opts = {
        "format": "bv*+ba/best",
        "outtmpl": str(out_template),
        "quiet": True,
        "no_warnings": True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.extract_info(url, download=True)

        candidates = [p for p in bvid_dir.glob(f"{bvid}.*") if p.suffix.lower() == ".mp4"]
        if not candidates:
            return None

        video_path = candidates[0]
        if video_path != cached_video:
            video_path.rename(cached_video)

        print(f"[{bvid}] 下载完成并缓存: {cached_video}")
        return cached_video
    except Exception as e:
        print(f"[{bvid}] 下载失败: {e}")
        return None


def ensure_cached_audio(bvid: str, cached_video: Path) -> Optional[Path]:
    bvid_dir = cached_video.parent
    cached_audio = bvid_dir / f"{bvid}.wav"
    if cached_audio.exists():
        return cached_audio

    cmd = [
        FFMPEG_BIN,
        "-y",
        "-i",
        str(cached_video),
        "-vn",
        "-ac",
        "1",
        "-ar",
        "22050",
        str(cached_audio),
        "-loglevel",
        "error",
    ]
    try:
        subprocess.run(cmd, check=True)
        return cached_audio
    except Exception:
        return None


def generate_clip_with_overlay(
    row: pd.Series,
    clip_index: int,
    clip_duration: float,
    issue_date_str: str,
    ) -> Optional[Path]:
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

    print(f"=== 处理中 #{clip_index} | {bvid} | {title} ===")

    VIDEOS_ROOT.mkdir(exist_ok=True)
    bvid_dir = VIDEOS_ROOT / bvid
    bvid_dir.mkdir(exist_ok=True)

    cached_video = bvid_dir / f"{bvid}.mp4"
    cached_segment = bvid_dir / f"{bvid}_10s.mp4"

    if cached_segment.exists():
        segment_source_path = cached_segment
    else:
        if not cached_video.exists():
            video_path = download_bilibili_video(bvid)
            if not video_path:
                return None
            if video_path != cached_video:
                shutil.copy2(video_path, cached_video)

        audio_path = ensure_cached_audio(bvid, cached_video)
        if not audio_path:
            return None

        try:
            start, _ = find_climax_segment(str(audio_path), clip_duration=clip_duration)
        except Exception:
            start = 0.0

        vf_filter = "fade=t=in:st=0:d=1,fade=t=out:st=9:d=1"
        af_filter = "afade=t=in:st=0:d=1,afade=t=out:st=9:d=1"

        segment_cmd = [
            FFMPEG_BIN,
            "-y",
            "-ss",
            f"{start:.3f}",
            "-t",
            f"{clip_duration:.3f}",
            "-i",
            str(cached_video),
            "-vf",
            vf_filter,
            "-af",
            af_filter,
        ]
        add_x264_encode_args(segment_cmd)
        segment_cmd += [
            "-avoid_negative_ts",
            "make_zero",
            "-movflags",
            "+faststart",
            str(cached_segment),
            "-loglevel",
            "error",
        ]
        try:
            subprocess.run(segment_cmd, check=True)
        except Exception:
            return None
        segment_source_path = cached_segment

    fontfile_expr = ffmpeg_escape_path(FONT_FILE)
    title_text = ffmpeg_escape(wrap_title(title))
    bvid_text = ffmpeg_escape(bvid)
    pubdate_text = ffmpeg_escape(pubdate)
    author_text = ffmpeg_escape(author)
    count_text = ffmpeg_escape(count)
    point_text = ffmpeg_escape(format_number(point))
    wm_line1 = ffmpeg_escape("术力口数据姬")
    wm_line2 = ffmpeg_escape("vocabili.top")

    if is_new:
        rank_text = ffmpeg_escape("NEW!!")
        rank_color = "#FF3333"
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

    def stat_pair(label: str, value) -> tuple[str, str]:
        return ffmpeg_escape(label), ffmpeg_escape(format_number(value))

    stats = [
        stat_pair("播放", view),
        stat_pair("收藏", favorite),
        stat_pair("硬币", coin),
        stat_pair("点赞", like),
        stat_pair("弹幕", danmaku),
        stat_pair("评论", reply),
        stat_pair("分享", share),
    ]

    left_text_filters = [
        (
            f"drawtext=fontfile='{fontfile_expr}':text='{title_text}':x=40:y=60:"
            f"fontsize=50:fontcolor=white:box=1:boxcolor=black@0.45:boxborderw=14:"
            f"shadowx=1:shadowy=1:shadowcolor=black@0.55"
        ),
        (
            f"drawtext=fontfile='{fontfile_expr}':text='{rank_text}':x=40:y=140:"
            f"fontsize=120:fontcolor={rank_color}:box=1:boxcolor=black@0.45:boxborderw=14:"
            f"shadowx=1:shadowy=1:shadowcolor=black@0.9"
        ),
        (
            f"drawtext=fontfile='{fontfile_expr}':text='{bvid_text}':x=40:y=280:"
            f"fontsize=32:fontcolor=white:shadowx=3:shadowy=3:shadowcolor=black@0.9"
        ),
        (
            f"drawtext=fontfile='{fontfile_expr}':text='{pubdate_text}':x=40:y=320:"
            f"fontsize=32:fontcolor=white:shadowx=3:shadowy=3:shadowcolor=black@0.9"
        ),
        (
            f"drawtext=fontfile='{fontfile_expr}':text='作者：{author_text}':x=40:y=360:"
            f"fontsize=32:fontcolor=white:shadowx=3:shadowy=3:shadowcolor=black@0.9"
        ),
        (
            f"drawtext=fontfile='{fontfile_expr}':text='上榜次数：{count_text}':x=40:y=400:"
            f"fontsize=32:fontcolor=white:shadowx=3:shadowy=3:shadowcolor=black@0.9"
        ),
    ]

    point_y = 140
    point_filter = (
        f"drawtext=fontfile='{fontfile_expr}':text='{point_text}':"
        f"x=w-tw-80:y={point_y}:fontsize=44:fontcolor=#FFD700:"
        f"shadowx=1:shadowy=1:shadowcolor=black@0.6"
    )

    label_x = "w-260"
    value_x = "w-80-tw"
    base_y = point_y + 70
    line_height = 32

    stat_filters: List[str] = []
    for idx, (label, value) in enumerate(stats):
        y = base_y + idx * line_height
        stat_filters.append(
            f"drawtext=fontfile='{fontfile_expr}':text='{label}':x={label_x}:y={y}:"
            f"fontsize=26:fontcolor=white:shadowx=2:shadowy=2:shadowcolor=black@0.9"
        )
        stat_filters.append(
            f"drawtext=fontfile='{fontfile_expr}':text='{value}':x={value_x}:y={y}:"
            f"fontsize=26:fontcolor=#FFD700:shadowx=2:shadowy=2:shadowcolor=black@0.5"
        )

    base_filters = [
        "[0:v]settb=AVTB,setpts=PTS-STARTPTS,setsar=1,fps=60[v0]",
        "[v0]scale=trunc(1920*a/2)*2:1920,setsar=1,"
        "crop=1080:1920:(in_w-1080)/2:(in_h-1920)/2,boxblur=20:8[bg]",
        "[v0]scale=1080:-1,setsar=1[fg]",
        "[bg][fg]overlay=(W-w)/2:(H-h)/2[vbase]",
        "[0:a]asetpts=PTS-STARTPTS[aout]",
    ]
    watermark_filters = [
        (
            f"drawtext=fontfile='{fontfile_expr}':text='{wm_line1}':"
            f"x=30:y=h-600:fontsize=36:fontcolor=white@0.65:"
            f"shadowx=1:shadowy=1:shadowcolor=black@0.65"
        ),
        (
            f"drawtext=fontfile='{fontfile_expr}':text='{wm_line2}':"
            f"x=30:y=h-560:fontsize=32:fontcolor=white@0.35:"
            f"shadowx=1:shadowy=1:shadowcolor=black@0.35"
        ),
    ]
    draw_ops = left_text_filters + [point_filter] + stat_filters + watermark_filters
    draw_filters: List[str] = []
    current_label = "vbase"
    for idx, expr in enumerate(draw_ops):
        next_label = f"vtxt{idx}"
        draw_filters.append(f"[{current_label}]{expr}[{next_label}]")
        current_label = next_label

    final_video_label = current_label
    filter_complex = ";".join(base_filters + draw_filters)

    DAILY_VIDEO_DIR.mkdir(exist_ok=True)
    clip_filename = DAILY_VIDEO_DIR / f"tmp_{issue_date_str}_{clip_index:02d}_{bvid}.mp4"

    cmd = [
        FFMPEG_BIN,
        "-y",
        "-ignore_editlist",
        "1",
        "-i",
        str(segment_source_path),
        "-filter_complex",
        filter_complex,
        "-map",
        f"[{final_video_label}]",
        "-map",
        "[aout]",
    ]
    add_x264_encode_args(cmd)
    cmd += [
        "-movflags",
        "+faststart",
        str(clip_filename),
        "-loglevel",
        "error",
    ]
    try:
        subprocess.run(cmd, check=True)
        print(f"[{bvid}] 竖屏片段生成完成: {clip_filename}")
        return clip_filename
    except subprocess.CalledProcessError:
        return None


def generate_cover_image_from_rows(combined_rows: List[pd.Series], cover_path: Path) -> None:
    best_index = None
    for idx, row in enumerate(combined_rows):
        if not bool(row.get("is_new", False)) and row.get("rank", None) == 1:
            best_index = idx
            break
    if best_index is None:
        best_index = 0

    new_indices = [idx for idx, row in enumerate(combined_rows) if bool(row.get("is_new", False))]
    new1_index, new2_index = new_indices[:2]

    used_indices = {best_index, new1_index, new2_index}
    remaining_indices = [idx for idx in range(len(combined_rows)) if idx not in used_indices]
    bottom_indices = random.sample(remaining_indices, 3)
    index_order = [best_index, new1_index, new2_index] + bottom_indices
    urls = [str(combined_rows[idx].get("image_url", "")).strip() for idx in index_order]

    cmd = [FFMPEG_BIN, "-y"]
    for url in urls:
        cmd += ["-i", url]

    def tf(stream_index: int, width: int, height: int, label: str) -> str:
        return (
            f"[{stream_index}:v]scale={width}:{height}:force_original_aspect_ratio=increase,"
            f"crop={width}:{height},setsar=1[{label}]"
        )

    filters = [
        tf(0, 1280, 720, "v0"),
        tf(1, 640, 360, "v1"),
        tf(2, 640, 360, "v2"),
        tf(3, 640, 360, "v3"),
        tf(4, 640, 360, "v4"),
        tf(5, 640, 360, "v5"),
        "[v0][v1][v2][v3][v4][v5]xstack=inputs=6:layout="
        "0_0|1280_0|1280_360|0_720|640_720|1280_720[vout]",
    ]

    cmd += [
        "-filter_complex",
        ";".join(filters),
        "-map",
        "[vout]",
        "-frames:v",
        "1",
        "-q:v",
        "2",
        str(cover_path),
        "-loglevel",
        "error",
    ]
    subprocess.run(cmd, check=True)
    print(f"封面图片已保存: {cover_path}")


def get_cover_image_pil(url: str, bvid: str) -> Image.Image:
    VIDEOS_ROOT.mkdir(exist_ok=True)
    cover_cache = VIDEOS_ROOT / bvid / "cover.jpg"
    if cover_cache.exists():
        try:
            return Image.open(cover_cache).convert("RGBA")
        except Exception:
            pass

    if url and url.startswith("http"):
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                img = Image.open(BytesIO(response.content)).convert("RGBA")
                if not cover_cache.parent.exists():
                    cover_cache.parent.mkdir(parents=True)
                img.convert("RGB").save(cover_cache)
                return img
        except Exception:
            pass
    return Image.new("RGBA", (300, 200), (200, 200, 200, 255))


def wrap_text_pil(text: str, font: ImageFont.FreeTypeFont, max_width: int) -> List[str]:
    lines: List[str] = []
    current_line = ""
    if not text:
        return lines

    for char in text:
        if font.getbbox(current_line + char)[2] <= max_width:
            current_line += char
        else:
            lines.append(current_line)
            current_line = char
    if current_line:
        lines.append(current_line)
    return lines


def create_card_image(row: pd.Series) -> Image.Image:
    title = str(row.get("title", ""))
    bvid = str(row.get("bvid", ""))
    author = str(row.get("author", ""))
    pubdate = str(row.get("pubdate", ""))
    image_url = str(row.get("image_url", ""))
    crossed_val = int(row.get("10w_crossed", 0))

    achievement_text = f"{crossed_val * 10}万播放达成!!"

    img = Image.new("RGBA", (CARD_W, CARD_H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    bg_color = (255, 255, 255, 255)
    draw.rounded_rectangle((0, 0, CARD_W, CARD_H), radius=CARD_RADIUS, fill=bg_color)

    margin = 20
    cover_height = CARD_H - 2 * margin
    cover_width = int(cover_height * (16 / 9))

    cover_img = get_cover_image_pil(image_url, bvid)
    cover_img = cover_img.resize((cover_width, cover_height), Image.Resampling.LANCZOS)

    mask = Image.new("L", (cover_width, cover_height), 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, cover_width, cover_height), radius=15, fill=255)
    img.paste(cover_img, (margin, margin), mask)

    text_x = margin + cover_width + 30
    text_y = margin + 5
    text_width = CARD_W - text_x - margin

    font_title = ImageFont.truetype(FONT_BOLD_FILE, 34)
    font_info = ImageFont.truetype(FONT_FILE, 22)
    font_author = ImageFont.truetype(FONT_BOLD_FILE, 28)
    font_achieve = ImageFont.truetype(FONT_BOLD_FILE, 54)

    title_lines = wrap_text_pil(title, font_title, text_width)
    for line in title_lines[:2]:
        draw.text((text_x, text_y), line, font=font_title, fill=(20, 20, 20))
        text_y += 45

    text_y += 5
    draw.text((text_x, text_y), f"{bvid}  {pubdate}", font=font_info, fill=(100, 100, 100))
    text_y += 32
    draw.text((text_x, text_y), f"作者：{author}", font=font_author, fill=(60, 60, 60))

    achieve_y = CARD_H - margin - 60
    shadow_color = "#CCAC00"
    main_color = "#FFD700"

    draw.text((text_x + 2, achieve_y + 2), achievement_text, font=font_achieve, fill=shadow_color)
    draw.text((text_x, achieve_y), achievement_text, font=font_achieve, fill=main_color)

    return img


def create_header_image(
    width: int,
    height: int,
    opacity: float,
    ed_info: Optional[Dict[str, str]] = None,
    list_top_y: Optional[int] = None,
    ) -> Image.Image:
    img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    title = "今日成就达成"
    title_font = ImageFont.truetype(FONT_BOLD_FILE, 80)

    title_bbox = title_font.getbbox(title)
    title_width = title_bbox[2] - title_bbox[0]
    title_height = title_bbox[3] - title_bbox[1]
    title_x = (width - title_width) // 2
    title_y = 150

    alpha = int(255 * opacity)
    if alpha <= 0:
        return img

    title_color = (0, 139, 139, alpha)
    title_shadow = (255, 255, 255, alpha)

    draw.text((title_x + 3, title_y + 3), title, font=title_font, fill=title_shadow)
    draw.text((title_x, title_y), title, font=title_font, fill=title_color)

    if ed_info:
        name = (ed_info.get("name") or "").strip()
        author = (ed_info.get("author") or "").strip()
        bvid = (ed_info.get("bvid") or "").strip()

        if name or author or bvid:
            line1 = "ED：" + (name if name else "")
            if author:
                line1 += f" / {author}"
            line2 = bvid

            try:
                ed_font = ImageFont.truetype(FONT_FILE, 32)
            except Exception:
                ed_font = title_font

            line1_bbox = ed_font.getbbox(line1)
            line2_bbox = ed_font.getbbox(line2)
            line1_width = line1_bbox[2] - line1_bbox[0]
            line1_height = line1_bbox[3] - line1_bbox[1]
            line2_width = line2_bbox[2] - line2_bbox[0]
            line2_height = line2_bbox[3] - line2_bbox[1]
            block_width = max(line1_width, line2_width)
            block_height = line1_height + line2_height + 10

            region_top = title_y + title_height + 25
            region_bottom = list_top_y if list_top_y is not None else int(height * 0.5)

            region_bottom = max(0, min(height, region_bottom))
            if region_bottom - region_top >= block_height + 10:
                ed_y = region_top + (region_bottom - region_top - block_height) // 2
            else:
                ed_y = region_top

            margin_right = 80
            ed_x = width - margin_right - block_width

            ed_color = (0, 0, 0, alpha)
            ed_shadow = (255, 255, 255, alpha)

            draw.text((ed_x + 2, ed_y + 2), line1, font=ed_font, fill=ed_shadow)
            draw.text((ed_x, ed_y), line1, font=ed_font, fill=ed_color)

            y2 = ed_y + line1_height + 4
            draw.text((ed_x + 2, y2 + 2), line2, font=ed_font, fill=ed_shadow)
            draw.text((ed_x, y2), line2, font=ed_font, fill=ed_color)

    return img


def generate_achievement_video(
    excel_path_10w: Path,
    output_video: Path,
    issue_index: int,
) -> bool:
    if not excel_path_10w.exists():
        print(f"十万记录 Excel 不存在: {excel_path_10w}")
        return False

    df_10w = pd.read_excel(excel_path_10w)
    if df_10w.empty:
        print("十万记录 Excel 为空, 无需生成成就视频。")
        return False

    if "10w_crossed" in df_10w.columns:
        df_10w = df_10w.sort_values("10w_crossed", ascending=False)

    achievement_rows = [row for _, row in df_10w.iterrows()]
    achievement_count = len(achievement_rows)
    print(f"开始生成十万成就滚动视频，条目数: {achievement_count}")

    cards: List[Image.Image] = []
    for row in achievement_rows:
        cards.append(create_card_image(row))

    strip_height = achievement_count * (CARD_H + CARD_GAP)
    full_strip = Image.new("RGBA", (1080, strip_height), (0, 0, 0, 0))

    for idx, card_img in enumerate(cards):
        y_pos = idx * (CARD_H + CARD_GAP)
        x_pos = (1080 - CARD_W) // 2
        full_strip.paste(card_img, (x_pos, y_pos))

    fps = 60
    width, height = 1080, 1920

    initial_list_top_y = 800
    total_scroll_distance = initial_list_top_y + strip_height

    scroll_duration = total_scroll_distance / SCROLL_SPEED_PPS
    total_duration = SCROLL_HOLD_TIME + scroll_duration + 0.5
    total_frames = int(total_duration * fps)

    ed_info = get_ed_info(issue_index)
    bgm_bvid = ed_info["bvid"] if ed_info and ed_info.get("bvid") else None
    audio_input_args: List[str] = []
    audio_map: Optional[str] = None

    if bgm_bvid:
        video_path = download_bilibili_video(bgm_bvid)
        if video_path:
            audio_path = ensure_cached_audio(bgm_bvid, video_path)
            if audio_path:
                try:
                    start_sec, _ = find_climax_segment(str(audio_path), clip_duration=total_duration)
                except Exception:
                    start_sec = 0.0

                audio_input_args = ["-ss", f"{start_sec:.3f}", "-i", str(audio_path)]
                audio_map = "1:a"
            else:
                print(f"ED BGM 音频提取失败: {bgm_bvid}")
        else:
            print(f"ED BGM 视频下载失败: {bgm_bvid}")

    if not audio_map:
        print("未找到有效 ED BGM，使用静音音轨作为成就视频背景。")
        audio_input_args = ["-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100"]
        audio_map = "1:a"

    cmd = [
        FFMPEG_BIN,
        "-y",
        "-f",
        "rawvideo",
        "-vcodec",
        "rawvideo",
        "-s",
        f"{width}x{height}",
        "-pix_fmt",
        "rgba",
        "-r",
        str(fps),
        "-i",
        "-",
    ]

    cmd += audio_input_args

    fade_out_start = max(0, total_duration - 1.0)
    af_str = f"afade=t=in:st=0:d=1,afade=t=out:st={fade_out_start:.3f}:d=1"

    cmd += [
        "-map",
        "0:v",
        "-map",
        audio_map,
        "-af",
        af_str,
    ]
    add_x264_encode_args(cmd)
    cmd += [
        "-t",
        f"{total_duration:.3f}",
        str(output_video),
        "-loglevel",
        "error",
    ]

    process = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    bg_base = Image.new("RGBA", (width, height), SCROLL_BG_COLOR)

    try:
        for frame_index in range(total_frames):
            t = frame_index / fps

            if t < SCROLL_HOLD_TIME:
                curr_strip_y = float(initial_list_top_y)
                header_opacity = 1.0
            else:
                scroll_t = t - SCROLL_HOLD_TIME
                curr_strip_y = initial_list_top_y - (scroll_t * SCROLL_SPEED_PPS)

                fade_duration = 1.5
                if scroll_t < fade_duration:
                    header_opacity = 1.0 - (scroll_t / fade_duration)
                else:
                    header_opacity = 0.0

            frame = bg_base.copy()
            paste_y = int(curr_strip_y)

            if paste_y < height and (paste_y + strip_height) > 0:
                frame.paste(full_strip, (0, paste_y), full_strip)

            if header_opacity > 0:
                header_img = create_header_image(width, height, header_opacity, ed_info, list_top_y=paste_y)
                frame.alpha_composite(header_img)

            process.stdin.write(frame.tobytes())

            if frame_index % 60 == 0:
                print(f"\r正在渲染十万成就视频: {t:.1f}/{total_duration:.1f}s", end="")

        process.stdin.close()
        process.wait()
        print("十万成就滚动视频生成完成。")
        return True

    except Exception as exc:
        print(f"生成十万成就视频时出错: {exc}")
        try:
            process.stdin.close()
        except Exception:
            pass
        return False


def concat_clips(clip_paths: List[Path], output_path: Path) -> None:
    if not clip_paths:
        return

    DAILY_VIDEO_DIR.mkdir(exist_ok=True)
    print("正在拼接所有片段...")

    cmd = [FFMPEG_BIN, "-y"]
    for path in clip_paths:
        cmd += ["-i", str(path)]

    clip_count = len(clip_paths)
    va_inputs = "".join(f"[{i}:v][{i}:a]" for i in range(clip_count))
    filter_complex = f"{va_inputs}concat=n={clip_count}:v=1:a=1[v][a]"

    cmd += [
        "-filter_complex",
        filter_complex,
        "-map",
        "[v]",
        "-map",
        "[a]",
    ]
    add_x264_encode_args(cmd)

    cmd += [
        "-movflags",
        "+faststart",
        str(output_path),
        "-loglevel",
        "error",
    ]

    try:
        subprocess.run(cmd, check=True)
        print(f"最终合成视频已保存: {output_path}")
    except subprocess.CalledProcessError as exc:
        print(f"片段拼接失败: {exc}")

    for path in clip_paths:
        try:
            path.unlink()
        except Exception:
            pass


def main() -> None:
    VIDEOS_ROOT.mkdir(exist_ok=True)
    DAILY_VIDEO_DIR.mkdir(exist_ok=True)

    excel_path = get_latest_excel(TOTAL_DIR)
    issue_date_str, issue_index, excel_date_str = infer_issue_info(excel_path)
    final_video_path = DAILY_VIDEO_DIR / f"{issue_index}_{issue_date_str}.mp4"

    df_total = pd.read_excel(excel_path)
    df_top = df_total.sort_values("rank", ascending=True).head(TOP_N)
    df_top = df_top.sort_values("rank", ascending=False).reset_index(drop=True)

    count_map: Dict[str, int] = {}
    if "count" in df_total.columns:
        for _, row in df_total.iterrows():
            bvid = str(row.get("bvid", "")).strip()
            if bvid:
                count_map[bvid] = row.get("count", 0)

    newsong_excel = get_newsong_excel_for(excel_path, NEW_SONG_DIR)
    print(f"使用新曲榜 Excel: {newsong_excel}")
    df_new = pd.read_excel(newsong_excel)
    df_new_sorted = df_new.sort_values("rank", ascending=True) if "rank" in df_new.columns else df_new

    top_bvids = {str(bvid).strip() for bvid in df_top["bvid"].astype(str)}
    new_rows_raw: List[pd.Series] = []
    for _, row in df_new_sorted.iterrows():
        bvid = str(row.get("bvid", "")).strip()
        if bvid and bvid not in top_bvids:
            new_rows_raw.append(row)
            if len(new_rows_raw) >= 2:
                break

    combined_rows: List[pd.Series] = []
    for row in new_rows_raw:
        series_copy = row.copy()
        series_copy["is_new"] = True
        bvid = str(series_copy.get("bvid", "")).strip()
        series_copy["count"] = count_map.get(bvid, 0) if bvid else 0
        combined_rows.append(series_copy)

    for _, row in df_top.iterrows():
        series_copy = row.copy()
        series_copy["is_new"] = False
        combined_rows.append(series_copy)

    tasks = [(idx + 1, row.to_dict()) for idx, row in enumerate(combined_rows)]
    index_to_path: Dict[int, Path] = {}

    def worker(idx_row: Tuple[int, dict]) -> Tuple[int, Optional[Path]]:
        idx, row_dict = idx_row
        series_row = pd.Series(row_dict)
        clip_path = generate_clip_with_overlay(
            row=series_row,
            clip_index=idx,
            clip_duration=CLIP_DURATION,
            issue_date_str=issue_date_str,
        )
        return idx, clip_path

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_task = {executor.submit(worker, task): task for task in tasks}
        for future in as_completed(future_to_task):
            idx, clip_path = future.result()
            if clip_path:
                index_to_path[idx] = clip_path

    if not index_to_path:
        print("没有成功生成任何视频片段，程序结束。")
        return

    all_clips = [index_to_path[i] for i in sorted(index_to_path.keys())]
    cover_path = DAILY_VIDEO_DIR / f"{issue_index}_{issue_date_str}_cover.jpg"
    generate_cover_image_from_rows(combined_rows, cover_path)
    cover_path_3_4 = DAILY_VIDEO_DIR / f"{issue_index}_{issue_date_str}_cover_3-4.jpg"
    generate_cover_image_3_4_from_rows(combined_rows, cover_path_3_4)
    achieve_excel_name = f"十万记录{excel_date_str}与{issue_date_str}.xlsx"
    achieve_excel_path = ACHIEVEMENT_DIR / achieve_excel_name
    achieve_video_path = DAILY_VIDEO_DIR / f"tmp_achievement_{issue_date_str}.mp4"

    print(f"尝试查找十万成就 Excel: {achieve_excel_path}")

    has_achievement = generate_achievement_video(
        achieve_excel_path,
        achieve_video_path,
        issue_index,
    )

    if has_achievement:
        print("检测到十万成就，附加成就视频到总榜视频末尾。")
        all_clips.append(achieve_video_path)

    concat_clips(all_clips, final_video_path)


if __name__ == "__main__":
    main()
