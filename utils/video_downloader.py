# utils/video_downloader.py
from pathlib import Path
from typing import Optional
import subprocess
from utils.logger import logger

class VideoDownloader:
    def __init__(self, videos_root: Path, ffmpeg_bin: str):
        self.videos_root = videos_root
        self.ffmpeg_bin = ffmpeg_bin
        self.videos_root.mkdir(parents=True, exist_ok=True)

    def download_video(self, bvid: str) -> Optional[Path]:
        import yt_dlp

        bvid_dir = self.videos_root / bvid
        bvid_dir.mkdir(exist_ok=True)

        cached_video = bvid_dir / f"{bvid}.mp4"
        if cached_video.exists():
            return cached_video

        logger.info(f"开始下载视频: {bvid}")
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

            logger.info(f"[{bvid}] 下载完成并缓存: {cached_video}")
            return cached_video
        except Exception as e:
            logger.error(f"[{bvid}] 下载失败: {e}")
            return None

    def ensure_audio(self, bvid: str, cached_video: Path) -> Optional[Path]:
        bvid_dir = cached_video.parent
        cached_audio = bvid_dir / f"{bvid}.wav"
        if cached_audio.exists():
            return cached_audio

        cmd = [
            self.ffmpeg_bin, "-y",
            "-i", str(cached_video),
            "-vn", "-ac", "1", "-ar", "22050",
            str(cached_audio), "-loglevel", "error",
        ]
        try:
            subprocess.run(cmd, check=True)
            return cached_audio
        except Exception:
            return None
