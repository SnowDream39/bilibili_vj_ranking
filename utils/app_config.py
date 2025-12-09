# utils/app_config.py
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
VIDEO_CONFIG_PATH = PROJECT_ROOT / "config" / "video.yaml"

@dataclass(frozen=True)
class PathsConfig:
    total_dir: Path
    newsong_dir: Path
    daily_video_dir: Path
    videos_root: Path
    achievement_dir: Path
    icon_dir: Path

@dataclass(frozen=True)
class FontsConfig:
    regular: str
    bold: str

@dataclass(frozen=True)
class FfmpegConfig:
    bin: str

@dataclass(frozen=True)
class VideoBasicConfig:
    top_n: int
    clip_duration: float
    first_issue_date: str

@dataclass(frozen=True)
class UiConfig:
    scroll_bg_color: tuple[int, int, int, int]
    card_width: int
    card_height: int
    card_gap: int
    card_radius: int
    scroll_hold_time: float
    scroll_speed_pps: float

@dataclass(frozen=True)
class AppConfig:
    project_root: Path
    paths: PathsConfig
    fonts: FontsConfig
    ffmpeg: FfmpegConfig
    video: VideoBasicConfig
    ui: UiConfig

def load_app_config(config_path: Path = VIDEO_CONFIG_PATH) -> AppConfig:
    """加载视频生成相关的应用配置"""
    with open(config_path, "r", encoding="utf-8") as f:
        raw: Dict[str, Any] = yaml.safe_load(f)

    p = raw["paths"]
    paths = PathsConfig(
        total_dir=PROJECT_ROOT / p["total_dir"],
        newsong_dir=PROJECT_ROOT / p["newsong_dir"],
        daily_video_dir=PROJECT_ROOT / p["daily_video_dir"],
        videos_root=PROJECT_ROOT / p["videos_root"],
        achievement_dir=PROJECT_ROOT / p["achievement_dir"],
        icon_dir=PROJECT_ROOT / p["icon_dir"],
    )

    f_cfg = raw["fonts"]
    fonts = FontsConfig(regular=str(f_cfg["regular"]), bold=str(f_cfg["bold"]))

    ffmpeg = FfmpegConfig(bin=str(raw["ffmpeg"]["bin"]))

    v = raw["video"]
    video = VideoBasicConfig(
        top_n=int(v["top_n"]),
        clip_duration=float(v["clip_duration"]),
        first_issue_date=str(v["first_issue_date"]),
    )

    u = raw["ui"]
    c = u["scroll_bg_color"]
    bg_color = tuple(c) if len(c) == 4 else (c[0], c[1], c[2], 255)
    
    ui = UiConfig(
        scroll_bg_color=bg_color,
        card_width=int(u["card_width"]),
        card_height=int(u["card_height"]),
        card_gap=int(u["card_gap"]),
        card_radius=int(u["card_radius"]),
        scroll_hold_time=float(u["scroll_hold_time"]),
        scroll_speed_pps=float(u["scroll_speed_pps"]),
    )

    return AppConfig(
        project_root=PROJECT_ROOT,
        paths=paths,
        fonts=fonts,
        ffmpeg=ffmpeg,
        video=video,
        ui=ui,
    )
