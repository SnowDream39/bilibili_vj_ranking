# src/daily_video_flow.py
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
from PIL import Image, ImageFont
from utils.config import load_app_config, AppConfig
from utils.logger import logger
from utils.climax_clipper import find_climax_segment
from utils.issue import Issue
from utils.video_downloader import VideoDownloader
from utils.cover import Cover
from utils.achievement_clipper import AchievementClipper
from src.clip_flow import ClipFlow

def ffmpeg_escape_path(path: str) -> str:
    p = path.replace("\\", "/")
    if ":" in p:
        drive, rest = p.split(":", 1)
        p = f"{drive}\\:{rest}"
    return p

class DailyVideoFlow:
    def __init__(self, cfg: AppConfig | None = None) -> None:
        self.cfg = cfg or load_app_config()

        self.video_downloader = VideoDownloader(
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
            card_radius=self.cfg.ui.card_radius
        )

        self.achieve_clipper = AchievementClipper(
            achievement_dir=self.cfg.paths.achievement_dir,
            config_dir=self.cfg.project_root / "config",
            image_factory=self.cover_mgr
        )

        self.clip_flow = ClipFlow(
            video_downloader=self.video_downloader,
            daily_video_dir=self.cfg.paths.daily_video_dir,
            icon_dir=self.cfg.paths.icon_dir,
            font_regular=self.cfg.fonts.regular,
            font_bold=self.cfg.fonts.bold,
            ffmpeg_bin=self.cfg.ffmpeg.bin
        )

        self.daily_video_dir = self.cfg.paths.daily_video_dir
        self.top_n = self.cfg.video.top_n
        self.clip_duration = self.cfg.video.clip_duration
        self.ffmpeg_bin = self.cfg.ffmpeg.bin

        c = self.cfg.ui.scroll_bg_color
        self.bg_color = tuple(c) if len(c) == 4 else (c[0], c[1], c[2], 255)
        self.ui = self.cfg.ui

    def run(self) -> None:
        self.daily_video_dir.mkdir(exist_ok=True)

        combined_rows, issue_date, issue_idx, excel_date = self._prepare_rows()
        
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
        """删除生成的临时片段文件"""
        for p in clip_paths:
            if p.exists():
                p.unlink()

    def _prepare_rows(self):
        excel_path = self.issue_mgr.get_latest_total_excel()
        issue_date, idx, ex_date = self.issue_mgr.infer_issue_info(excel_path)

        df_total = pd.read_excel(excel_path)
        df_top = df_total.sort_values("rank").head(self.top_n).sort_values("rank", ascending=False)
        count_map = {str(r['bvid']).strip(): r['count'] for _, r in df_total.iterrows() if pd.notna(r['bvid'])}

        newsong_path = self.issue_mgr.get_newsong_excel(excel_path)
        df_new = pd.read_excel(newsong_path)
        if "rank" in df_new.columns:
            df_new = df_new.sort_values("rank")

        top_bvids = set(df_top["bvid"].astype(str).str.strip())
        new_rows = []
        for _, row in df_new.iterrows():
            if str(row['bvid']).strip() not in top_bvids:
                new_rows.append(row)
                if len(new_rows) >= 2:
                    break

        combined = []
        for r in reversed(new_rows):
            s = r.copy()
            s['is_new'] = True
            s['count'] = count_map.get(str(s['bvid']).strip(), 0)
            combined.append(s)
        for _, r in df_top.iterrows():
            s = r.copy()
            s['is_new'] = False
            combined.append(s)

        return combined, issue_date, idx, ex_date

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
        self._ffmpeg_grid_cover(urls_16_9, self.daily_video_dir / f"{idx}_{date_str}_cover.jpg")
        urls_3_4 = self.cover_mgr.select_cover_urls_3_4(rows)
        self._ffmpeg_3_4_cover(urls_3_4, self.daily_video_dir / f"{idx}_{date_str}_cover_3-4.jpg")

    def _ffmpeg_grid_cover(self, urls, output):
        """
        横屏封面生成 (16:9)
        修复：移除了不被支持的 'r' 参数
        """
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
        
        font_path = ffmpeg_escape_path(self.cfg.fonts.bold)
        
        text1 = "虚拟歌手日刊"
        text2 = "外语排行榜"
        
        fill_color = "white@0.95"
        border_color = "#55CCCC"
        border_w = 12         
        font_size_1 = 160     
        font_size_2 = 110
        
        font_obj = ImageFont.truetype(self.cfg.fonts.bold, font_size_1)
        w1 = font_obj.getlength(text1)
            
        right_anchor_x = (1920 / 2) + (w1 / 2)
        
        base_y = 550 
        
        draw_t1 = (
            f"drawtext=fontfile='{font_path}':text='{text1}':"
            f"fontsize={font_size_1}:"
            f"fontcolor={fill_color}:"
            f"borderw={border_w}:bordercolor={border_color}:"
            f"x={right_anchor_x}-tw:y={base_y}:"
            f"shadowx=5:shadowy=5:shadowcolor=black@0.5" 
        )
        
        gap = 20
        draw_t2 = (
            f"drawtext=fontfile='{font_path}':text='{text2}':"
            f"fontsize={font_size_2}:"
            f"fontcolor={fill_color}:"
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
            str(output),
            "-loglevel", "error",
        ]

        try:
            subprocess.run(cmd, check=True)
            logger.info(f"封面图片已保存: {output}")
        except subprocess.CalledProcessError as e:
            logger.error(f"封面图片生成失败: {e}")

    def _ffmpeg_3_4_cover(self, urls, output):
        if not urls:
            logger.warning("3:4 封面生成失败：没有可用的封面 URL")
            return
        valid_urls = urls[:6]
        count = len(valid_urls)

        cmd = [self.ffmpeg_bin, "-y"]
        for u in valid_urls:
            cmd += ["-i", u]

        W = 1920
        H = 2560
        
        font_path = ffmpeg_escape_path(self.cfg.fonts.bold)

        bg_filter = (
            f"[0:v]scale={W}:{H}:force_original_aspect_ratio=increase,"
            f"crop={W}:{H},setsar=1,"
            f"boxblur=40:5,"
            f"eq=brightness=-0.1:saturation=1.3[bg]"
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
            
            pw = tw + 2 * pad
            ph = th + 2 * pad
            
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
            filters.append(
                f"[{current_bg}][{lbl}]overlay=x={pos['x']}:y={pos['y']}[{next_bg}]"
            )
            current_bg = next_bg
            
        hero_lbl = processed_labels[0]
        
        filters.append(
            f"[{hero_lbl}]split[h_src][h_sh_raw];"
            f"[h_sh_raw]drawbox=c=black:t=fill,"
            f"format=rgba,"
            f"gblur=sigma=40,"
            f"colorchannelmixer=aa=0.45[hero_shadow]"
        )
        
        filters.append(
            f"[{current_bg}][hero_shadow]overlay=x={hero_x}+30:y={hero_y}+40[bg_w_shadow]"
        )
        
        filters.append(
            f"[bg_w_shadow][h_src]overlay=x={hero_x}:y={hero_y}[combined_img]"
        )
        
        text1 = "虚拟歌手日刊"
        text2 = "外语排行榜"
        fill_color = "white@0.95"
        border_color = "#55CCCC"
        border_w = 22
        font_size_1 = 260  
        font_size_2 = 220
        title_base_y = 220
        
        font_obj = ImageFont.truetype(self.cfg.fonts.bold, font_size_1)
        w1 = font_obj.getlength(text1)
            
        right_anchor_x = (W / 2) + (w1 / 2)
        
        draw_t1 = (
            f"drawtext=fontfile='{font_path}':text='{text1}':"
            f"fontsize={font_size_1}:"
            f"fontcolor={fill_color}:"
            f"borderw={border_w}:bordercolor={border_color}:"
            f"x={right_anchor_x}-tw:y={title_base_y}:"
            f"shadowx=8:shadowy=8:shadowcolor=black@0.4"
        )
        
        gap = 40
        draw_t2 = (
            f"drawtext=fontfile='{font_path}':text='{text2}':"
            f"fontsize={font_size_2}:"
            f"fontcolor={fill_color}:"
            f"borderw={border_w}:bordercolor={border_color}:"
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
            str(output),
            "-loglevel", "error",
        ]

        try:
            subprocess.run(cmd, check=True)
            logger.info(f"3:4 封面图片已保存: {output}")
        except subprocess.CalledProcessError as e:
            logger.error(f"3:4 封面图片生成失败: {e}")

    def _generate_achievement_video(self, ex_date, is_date, idx) -> Optional[Path]:
        rows = self.achieve_clipper.load_rows(ex_date, is_date)
        if not rows:
            return None

        out_path = self.daily_video_dir / f"tmp_achievement_{is_date}.mp4"

        strip_img, strip_h = self.achieve_clipper.build_strip(
            rows, width=1080, gap=self.ui.card_gap
        )

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
            v_path = self.video_downloader.download_video(bgm_bvid)
            if v_path:
                a_path = self.video_downloader.ensure_audio(bgm_bvid, v_path)
                if a_path:
                    try:
                        start, _ = find_climax_segment(str(a_path), clip_duration=total_duration)
                    except:
                        start = 0.0
                    audio_input_args = ["-ss", f"{start:.3f}", "-i", str(a_path)]
                    audio_map = "1:a"
                else:
                    logger.warning(f"ED BGM 音频提取失败: {bgm_bvid}")
            else:
                logger.warning(f"ED BGM 视频下载失败: {bgm_bvid}")

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
                    logger.info(f"正在处理成就: {t:.1f}/{total_duration:.1f}s")

            process.stdin.close()
            process.wait()
            logger.info("成就视频生成完成。")
            return out_path

        except Exception as exc:
            logger.error(f"生成成就视频时出错: {exc}")
            try:
                process.stdin.close()
            except:
                pass
            return None

    def _concat_clips(self, clip_paths: List[Path], output_path: Path) -> None:
        if not clip_paths:
            return

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
