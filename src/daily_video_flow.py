# src/daily_video_flow.py
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
from PIL import Image
from utils.app_config import load_app_config, AppConfig
from utils.logger import logger
from utils.climax_clipper import find_climax_segment
from utils.issue import Issue
from utils.cover import Cover
from utils.achievement_clipper import AchievementClipper
from utils.dataclass import Config as ScraperConfig 
from src.bilibili_api_client import BilibiliApiClient
from src.clip_flow import ClipFlow

class DailyVideoFlow:
    def __init__(self, cfg: AppConfig | None = None) -> None:
        self.cfg = cfg or load_app_config()
        self.api_client = BilibiliApiClient(
            config=ScraperConfig(), 
            videos_root=self.cfg.paths.videos_root,
            ffmpeg_bin=self.cfg.ffmpeg.bin
        )

        self.issue_mgr = Issue(
            total_dir=self.cfg.paths.total_dir,
            newsong_dir=self.cfg.paths.newsong_dir,
            first_issue_date=self.cfg.video.first_issue_date
        )
        
        self.cover_mgr = Cover(
            videos_root=self.cfg.paths.videos_root,
            font_regular=self.cfg.fonts.regular,
            font_bold=self.cfg.fonts.bold,
            card_width=self.cfg.ui.card_width,
            card_height=self.cfg.ui.card_height,
            card_radius=self.cfg.ui.card_radius,
            ffmpeg_bin=self.cfg.ffmpeg.bin
        )

        self.achieve_clipper = AchievementClipper(
            achievement_dir=self.cfg.paths.achievement_dir,
            config_dir=self.cfg.project_root / "config",
            image_factory=self.cover_mgr
        )

        self.clip_flow = ClipFlow(
            api_client=self.api_client,
            daily_video_dir=self.cfg.paths.daily_video_dir,
            icon_dir=self.cfg.paths.icon_dir,
            font_regular=self.cfg.fonts.regular,
            font_bold=self.cfg.fonts.bold,
            ffmpeg_bin=self.cfg.ffmpeg.bin
        )

        self.daily_video_dir = self.cfg.paths.daily_video_dir
        self.clip_duration = self.cfg.video.clip_duration
        self.ffmpeg_bin = self.cfg.ffmpeg.bin

        c = self.cfg.ui.scroll_bg_color
        self.bg_color = tuple(c) if len(c) == 4 else (c[0], c[1], c[2], 255)
        self.ui = self.cfg.ui

    async def close(self):
        """关闭资源"""
        await self.api_client.close_session()

    def run(self) -> None:
        self.daily_video_dir.mkdir(exist_ok=True)

        combined_rows, issue_date, issue_idx, excel_date = self.issue_mgr.prepare_video_data(self.cfg.video.top_n)
        
        index_to_path = self._generate_clips(combined_rows, issue_date)
        if not index_to_path:
            logger.error("没有生成视频片段")
            return
        
        all_clips = [index_to_path[i] for i in sorted(index_to_path.keys())]
        
        self._generate_covers(combined_rows, issue_date, issue_idx)
        
        achieve_vid = self._generate_achievement_video(excel_date, issue_date, issue_idx)
        if achieve_vid:
            all_clips.append(achieve_vid)

        final_path = self.daily_video_dir / f"{issue_idx}_{issue_date}.mp4"
        self._concat_clips(all_clips, final_path)
        logger.info(f"完成: {final_path}")
        self._cleanup_temp_files(all_clips)

    def _cleanup_temp_files(self, clip_paths: List[Path]) -> None:
        for p in clip_paths:
            if p.exists():
                p.unlink()
        temp_text_root = self.daily_video_dir / "temp_texts"
        if temp_text_root.exists():
            shutil.rmtree(temp_text_root)

    def _generate_clips(self, rows, issue_date) -> Dict[int, Path]:
        tasks = [(i + 1, r.to_dict()) for i, r in enumerate(rows)]
        res = {}
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = {ex.submit(self._worker, t, issue_date): t for t in tasks}
            for f in as_completed(futures):
                idx, path = f.result()
                if path:
                    res[idx] = path
        return res

    def _worker(self, task, issue_date):
        idx, r_dict = task
        row = pd.Series(r_dict)
        
        current_duration = self.clip_duration
        is_new = bool(row.get("is_new", False))
        rank_val = row.get("rank", 999)
        
        try:
            rank_val = int(rank_val)
        except:
            rank_val = 999
            
        if not is_new and rank_val <= 3:
            current_duration = 20.0
            
        path = self.clip_flow.generate_clip(row, idx, current_duration, issue_date)
        return idx, path

    def _generate_covers(self, rows, date_str, idx):
        urls_16_9 = self.cover_mgr.select_cover_urls_grid(rows)
        self.cover_mgr.generate_grid_cover(
            urls_16_9, 
            self.daily_video_dir / f"{idx}_{date_str}_cover.jpg",
            issue_date=date_str, 
            issue_index=idx
        )
        
        urls_3_4 = self.cover_mgr.select_cover_urls_3_4(rows)
        self.cover_mgr.generate_vertical_cover(
            urls_3_4, 
            self.daily_video_dir / f"{idx}_{date_str}_cover_3-4.jpg",
            issue_date=date_str,
            issue_index=idx
        )
    def _generate_achievement_video(self, ex_date, is_date, idx) -> Optional[Path]:
        rows = self.achieve_clipper.load_rows(ex_date, is_date)

        out_path = self.daily_video_dir / f"tmp_achievement_{is_date}.mp4"
        strip_img, strip_h = self.achieve_clipper.build_strip(rows, width=1080, gap=self.ui.card_gap)

        initial_list_top_y = 800
        total_dist = initial_list_top_y + strip_h
        scroll_duration = total_dist / self.ui.scroll_speed_pps
        total_duration = self.ui.scroll_hold_time + scroll_duration + 0.5
        fps = 60
        total_frames = int(total_duration * fps)

        ed_info = self.achieve_clipper.get_ed_info(idx)
        bgm_bvid = ed_info.get("bvid")

        audio_input_args = ["-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100"]
        audio_map = "1:a"

        if bgm_bvid:
            v_path = self.api_client.download_video(bgm_bvid)
            if v_path:
                a_path = self.api_client.ensure_audio(bgm_bvid, v_path)
                if a_path:
                    start, _ = find_climax_segment(str(a_path), clip_duration=total_duration)
                    audio_input_args = ["-ss", f"{start:.3f}", "-i", str(a_path)]
                    audio_map = "1:a"

        width, height = 1080, 1920
        cmd = [
            self.ffmpeg_bin, "-y",
            "-f", "rawvideo",
            "-vcodec", "rawvideo",
            "-s", f"{width}x{height}",
            "-pix_fmt", "rgba",
            "-r", str(fps),
            "-i", "-",
        ]
        cmd += audio_input_args
        fade_out_start = max(0, total_duration - 1.0)
        af_str = f"afade=t=in:st=0:d=1,afade=t=out:st={fade_out_start:.3f}:d=1"

        cmd += [
            "-map", "0:v",
            "-map", audio_map,
            "-af", af_str,
        ]
        self._add_x264_encode_args(cmd)
        cmd += [
            "-t", f"{total_duration:.3f}",
            str(out_path),
            "-loglevel", "error",
        ]

        process = subprocess.Popen(cmd, stdin=subprocess.PIPE)
        bg_base = Image.new("RGBA", (width, height), self.bg_color)

        try:
            for frame_index in range(total_frames):
                t = frame_index / fps
                if t < self.ui.scroll_hold_time:
                    curr_strip_y = float(initial_list_top_y)
                    header_opacity = 1.0
                else:
                    scroll_t = t - self.ui.scroll_hold_time
                    curr_strip_y = initial_list_top_y - (scroll_t * self.ui.scroll_speed_pps)
                    fade_duration = 1.5
                    if scroll_t < fade_duration:
                        header_opacity = 1.0 - (scroll_t / fade_duration)
                    else:
                        header_opacity = 0.0

                frame = bg_base.copy()
                paste_y = int(curr_strip_y)
                if paste_y < height and (paste_y + strip_h) > 0:
                    frame.paste(strip_img, (0, paste_y), strip_img)
                if header_opacity > 0:
                    header_img = self.cover_mgr.create_header(
                        width, height, header_opacity,
                        ed_info=ed_info,
                        list_top_y=paste_y
                    )
                    frame.alpha_composite(header_img)
                process.stdin.write(frame.tobytes())

                if frame_index % 60 == 0:
                    logger.info(f"处理成就: {t:.1f}/{total_duration:.1f}s")

            process.stdin.close()
            process.wait()
            return out_path
        except Exception as exc:
            logger.error(f"成就视频出错: {exc}")
            try:
                process.stdin.close()
            except:
                pass
            return None

    def _concat_clips(self, clip_paths: List[Path], output_path: Path) -> None:
        cmd = [self.ffmpeg_bin, "-y"]
        for p in clip_paths:
            cmd += ["-i", str(p)]

        n = len(clip_paths)
        va = "".join(f"[{i}:v][{i}:a]" for i in range(n))
        filter_complex = f"{va}concat=n={n}:v=1:a=1[v][a]"
        cmd += ["-filter_complex", filter_complex, "-map", "[v]", "-map", "[a]"]
        self._add_x264_encode_args(cmd)
        cmd += ["-movflags", "+faststart", str(output_path), "-loglevel", "error"]
        subprocess.run(cmd, check=True)

    def _add_x264_encode_args(self, cmd: List[str]) -> None:
        cmd.extend([
            "-c:v", "libx264", "-crf", "16", "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "192k"
        ])
