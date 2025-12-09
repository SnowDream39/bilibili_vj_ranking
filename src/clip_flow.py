# src/clip_flow.py

from pathlib import Path
from typing import Optional
import subprocess
import pandas as pd
from utils.logger import logger
from src.bilibili_api_client import BilibiliApiClient
from utils.climax_clipper import find_climax_segment
from utils.clip_overlay import build_clip_overlay_cmd

class ClipFlow:
    def __init__(
        self,
        *,
        api_client: BilibiliApiClient,
        daily_video_dir: Path,
        icon_dir: Path,
        ffmpeg_bin: str,
        font_regular: str,
        font_bold: str,
    ) -> None:
        self.api_client = api_client
        self.daily_video_dir = daily_video_dir
        self.icon_dir = icon_dir
        self.ffmpeg_bin = ffmpeg_bin
        self.font_file = font_regular
        self.font_bold_file = font_bold

    def _add_x264_encode_args(self, cmd: list[str]) -> None:
        cmd += [
            "-c:v", "libx264",
            "-crf", "16",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac",
            "-b:a", "192k",
        ]

    def _ensure_segment(self, bvid: str, clip_duration: float) -> Optional[Path]:
        cached_video = self.api_client.download_video(bvid)
        if not cached_video:
            return None

        bvid_dir = cached_video.parent
        cached_segment = bvid_dir / f"{bvid}_{int(clip_duration)}s.mp4"

        if cached_segment.exists():
            return cached_segment

        audio_path = self.api_client.ensure_audio(bvid, cached_video)
        if not audio_path:
            return None

        try:
            start, _ = find_climax_segment(str(audio_path), clip_duration=clip_duration)
        except Exception:
            start = 0.0

        out_start = max(clip_duration - 1.0, 0.0)
        vf_filter = (
            f"fade=t=in:st=0:d=1,"
            f"fade=t=out:st={out_start:.3f}:d=1"
        )
        af_filter = (
            f"afade=t=in:st=0:d=1,"
            f"afade=t=out:st={out_start:.3f}:d=1"
        )

        segment_cmd = [
            self.ffmpeg_bin,
            "-y",
            "-ss", f"{start:.3f}",
            "-t", f"{clip_duration:.3f}",
            "-i", str(cached_video),
            "-vf", vf_filter,
            "-af", af_filter,
        ]
        self._add_x264_encode_args(segment_cmd)
        segment_cmd += [
            "-avoid_negative_ts", "make_zero",
            "-movflags", "+faststart",
            str(cached_segment),
            "-loglevel", "error",
        ]

        try:
            subprocess.run(segment_cmd, check=True)
        except Exception:
            return None

        return cached_segment

    def generate_clip(
        self,
        row: pd.Series,
        clip_index: int,
        clip_duration: float,
        issue_date_str: str,
    ) -> Optional[Path]:
        bvid = str(row.get("bvid", "")).strip()
        if not bvid:
            return None

        logger.info(f"处理 #{clip_index} | {row.get('title', '')}")

        segment_path = self._ensure_segment(bvid, clip_duration=clip_duration)
        if not segment_path:
            return None

        overlay_args, clip_filename = build_clip_overlay_cmd(
            segment_source_path=segment_path,
            row=row,
            clip_index=clip_index,
            issue_date_str=issue_date_str,
            daily_video_dir=self.daily_video_dir,
            icon_dir=self.icon_dir,
            font_file=self.font_file,
        )

        cmd = [self.ffmpeg_bin] + overlay_args
        self._add_x264_encode_args(cmd)
        cmd += [
            "-movflags", "+faststart",
            str(clip_filename),
            "-loglevel", "error",
        ]

        try:
            subprocess.run(cmd, check=True)
        except Exception:
            return None

        return clip_filename
