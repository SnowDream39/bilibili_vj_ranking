# utils/climax_clipper.py
import subprocess
from pathlib import Path
from typing import Tuple, Optional, Dict, List
import librosa
import numpy as np
import yt_dlp

def _normalize(x: np.ndarray) -> np.ndarray:
    """将数组线性归一化到 [0, 1] 区间。"""
    x = x.astype(float)
    min_v = x.min()
    max_v = x.max()
    if max_v - min_v < 1e-8:
        return np.zeros_like(x)
    return (x - min_v) / (max_v - min_v)


def _compute_block_chroma_repetition(
    y: np.ndarray,
    sr: int,
    block_sec: float = 1.0
) -> Tuple[np.ndarray, np.ndarray]:
    """
    基于色度特征(chroma)的块级重复度估计。

    按 block_sec 划分为若干音频块，对每块计算平均 chroma，
    通过块间余弦相似度估计该块在整首歌中的重复程度。
    """
    block_size = int(block_sec * sr)
    n_blocks = len(y) // block_size
    if n_blocks < 4:
        return np.zeros(n_blocks), np.linspace(block_sec / 2, n_blocks * block_sec - block_sec / 2, n_blocks)

    chroma_blocks: List[np.ndarray] = []
    for i in range(n_blocks):
        seg = y[i * block_size:(i + 1) * block_size]
        if len(seg) < block_size // 2:
            break
        chroma = librosa.feature.chroma_cqt(y=seg, sr=sr)
        chroma_mean = chroma.mean(axis=1)
        norm = np.linalg.norm(chroma_mean) + 1e-8
        chroma_blocks.append(chroma_mean / norm)

    chroma_blocks_arr = np.stack(chroma_blocks, axis=0)  # (B, 12)
    sim = chroma_blocks_arr @ chroma_blocks_arr.T        # (B, B)
    rep_score = sim.sum(axis=1) - 1.0                    # 去掉自身相似度
    rep_score = _normalize(rep_score)

    block_times = (np.arange(len(rep_score)) + 0.5) * block_sec
    return rep_score, block_times


def find_climax_segment(
    audio_path: str,
    clip_duration: float = 20.0,
    hop_length: int = 512
) -> Tuple[float, float]:
    """
    基于多特征得分 + 节拍/起音对齐的高潮片段检测。
    """
    y, sr = librosa.load(audio_path, sr=None, mono=True)

    # 帧级特征: RMS(响度) + onset envelope(节奏强度)
    rms = librosa.feature.rms(y=y, hop_length=hop_length)[0]
    onset_env = librosa.onset.onset_strength(y=y, sr=sr, hop_length=hop_length)

    min_len = min(len(rms), len(onset_env))
    rms = rms[:min_len]
    onset_env = onset_env[:min_len]

    rms_n = _normalize(rms)
    onset_n = _normalize(onset_env)

    # 块级重复度: 类似“副歌度”
    rep_block, block_times = _compute_block_chroma_repetition(y, sr, block_sec=1.0)
    if len(rep_block) == 0:
        rep_block = np.zeros(1)
        block_times = np.array([0.5])

    frame_idx = np.arange(min_len)
    frame_times = librosa.frames_to_time(frame_idx, sr=sr, hop_length=hop_length)
    rep_frame = np.interp(frame_times, block_times, rep_block)
    rep_n = _normalize(rep_frame)

    # 响度 + 节奏起伏 + 重复度
    alpha_rms = 0.45
    alpha_onset = 0.35
    alpha_rep = 0.20    
    combined_score = alpha_rms * rms_n + alpha_onset * onset_n + alpha_rep * rep_n

    frames_per_sec = sr / hop_length
    window_frames = int(clip_duration * frames_per_sec)
    duration = len(y) / sr

    if window_frames <= 1 or window_frames >= len(combined_score):
        # 过短直接裁整曲或前 clip_duration 秒
        return 0.0, min(float(duration), clip_duration)

    # 滑动窗口打分: mode='valid' 对应每个“窗口起点”的得分
    kernel = np.ones(window_frames, dtype=float)
    window_scores = np.convolve(combined_score, kernel, mode="valid")
    start_frame_idx = np.arange(len(window_scores))
    start_times = librosa.frames_to_time(start_frame_idx, sr=sr, hop_length=hop_length)

    # 位置约束: 避开开头和结尾的非高潮区域
    start_margin = 5.0   # 前 5 秒不作为窗口起点
    end_margin = 5.0     # 末尾保留 5 秒不作为窗口起点
    max_ratio = 2.0 / 3.0 
    total_margin = start_margin + end_margin
    if duration <= clip_duration + total_margin:
        min_start = 0.0
        max_start = max(0.0, duration - clip_duration)
    else:
        min_start = start_margin
        max_start_tail = duration - clip_duration - end_margin
        max_start_ratio = duration * max_ratio
        max_start = min(max_start_tail, max_start_ratio)
        max_start = max(min_start, max_start)

    valid_mask = (start_times >= min_start) & (start_times <= max_start)
    masked_scores = window_scores.copy()
    masked_scores[~valid_mask] = -1e18

    best_idx = int(np.argmax(masked_scores))
    rough_start = float(start_times[best_idx])
    rough_start = max(0.0, min(rough_start, max_start))
    rough_end = rough_start + clip_duration
    if rough_end > duration:
        rough_start = max(0.0, duration - clip_duration)
        rough_end = duration

    # 节拍/起音对齐
    search_back = 1.0      # 允许向前回溯的最大时间(秒)
    search_forward = 1.0   # 向后搜索节拍/起音的最大时间(秒)

    # 优先使用节拍对齐
    tempo, beat_frames = librosa.beat.beat_track(
        onset_envelope=onset_env,
        sr=sr,
        hop_length=hop_length
    )
    beat_times = librosa.frames_to_time(beat_frames, sr=sr, hop_length=hop_length)
    beat_mask = (beat_times >= rough_start - search_back) & (beat_times <= rough_start + search_forward)
    candidate_beats = beat_times[beat_mask]

    final_start = rough_start
    if len(candidate_beats) > 0:
        # 优先选择略不早于 rough_start 的节拍，没有则在候选中就近选择
        near_beats = candidate_beats[candidate_beats >= rough_start - 0.05]
        if len(near_beats) == 0:
            near_beats = candidate_beats
        final_start = float(near_beats[np.argmin(np.abs(near_beats - rough_start))])
    else:
        # 无明显节拍时退回起音对齐
        onset_frames = librosa.onset.onset_detect(
            onset_envelope=onset_env,
            sr=sr,
            hop_length=hop_length,
            units="frames"
        )
        onset_times = librosa.frames_to_time(onset_frames, sr=sr, hop_length=hop_length)
        onset_mask = (onset_times >= rough_start - search_back) & (onset_times <= rough_start + search_forward)
        candidate_onsets = onset_times[onset_mask]
        if len(candidate_onsets) > 0:
            final_start = float(candidate_onsets[np.argmin(np.abs(candidate_onsets - rough_start))])

    final_start = max(0.0, min(final_start, max_start))
    # 略微提前，避免切掉音头
    final_start = max(0.0, final_start - 0.03)
    final_end = min(float(duration), final_start + clip_duration)

    return float(final_start), float(final_end)

class BilibiliClimaxClipper:
    """
    从 B 站 BV 号下载视频，自动检测高潮片段并导出带淡入/淡出的剪辑。
    """

    def __init__(self, output_dir: str = "output", temp_dir: str = "temp", ffmpeg_bin: str = "ffmpeg") -> None:
        self.output_dir = Path(output_dir)
        self.temp_dir = Path(temp_dir)
        self.ffmpeg_bin = ffmpeg_bin
        self.output_dir.mkdir(exist_ok=True)
        self.temp_dir.mkdir(exist_ok=True)

    def extract_audio(self, video_path: str) -> str:
        """
        从视频中提取用于分析的音频（单声道，22.05kHz）。
        """
        audio_path = self.temp_dir / "analysis_audio.wav"
        cmd = [
            self.ffmpeg_bin, "-y",
            "-i", str(video_path),
            "-vn", "-ac", "1", "-ar", "22050",
            str(audio_path),
            "-loglevel", "error",
        ]
        subprocess.run(cmd, check=True)
        return str(audio_path)

    def clip_video(self, input_path: str, output_path: Path, start: float, duration: float) -> None:
        """
        按指定起点和时长裁剪视频。
        """
        fade_in_start = 0.0
        fade_in_duration = 1.0
        fade_out_start = max(0.0, duration - 1.0)
        fade_out_duration = 1.0

        vf_filter = (
            f"fade=t=in:st={fade_in_start}:d={fade_in_duration},"
            f"fade=t=out:st={fade_out_start:.3f}:d={fade_out_duration}"
        )
        af_filter = (
            f"afade=t=in:st={fade_in_start}:d={fade_in_duration},"
            f"afade=t=out:st={fade_out_start:.3f}:d={fade_out_duration}"
        )

        cmd = [
            self.ffmpeg_bin, "-y",
            "-ss", f"{start:.3f}",
            "-t", f"{duration:.3f}",
            "-i", str(input_path),
            "-vf", vf_filter,
            "-af", af_filter,
            "-c:v", "libx264", "-preset", "fast", "-crf", "20",
            "-c:a", "aac", "-b:a", "192k",
            "-avoid_negative_ts", "make_zero",
            "-movflags", "+faststart",
            str(output_path),
            "-loglevel", "error",
        ]
        subprocess.run(cmd, check=True)
        print(f"Clip saved: {output_path}")

    def clean_temp(self, *paths: Optional[str | Path]) -> None:
        """
        删除中间文件。
        """
        for p in paths:
            if not p:
                continue
            path_obj = Path(p)
            if path_obj.exists():
                try:
                    path_obj.unlink()
                except Exception:
                    pass

    def process_bvid(self, bvid: str, clip_duration: float = 10.0) -> Optional[Path]:
        """
        处理单个 BV 号，返回生成剪辑文件的路径；失败时返回 None。
        """
        video_path: Optional[str] = None
        audio_path: Optional[str] = None
        output_path: Optional[Path] = None

        try:
            video_path = self.download_bilibili_video(bvid)
            if not video_path:
                return None

            audio_path = self.extract_audio(video_path)
            start, end = find_climax_segment(audio_path, clip_duration=clip_duration)
            print(f"Detected climax for {bvid}: {start:.2f}s ~ {end:.2f}s")

            output_path = self.output_dir / f"{bvid}_{int(clip_duration)}s.mp4"
            self.clip_video(video_path, output_path, start, clip_duration)
            return output_path

        except Exception as e:
            print(f"Processing failed for {bvid}: {e}")
            return None
        finally:
            self.clean_temp(audio_path)

    def process_bvids(self, bvids: List[str], clip_duration: float = 20.0) -> Dict[str, Optional[Path]]:
        """
        批量处理多个 BV 号，返回 {bvid: 输出路径 or None}。
        """
        results: Dict[str, Optional[Path]] = {}
        for bvid in bvids:
            results[bvid] = self.process_bvid(bvid, clip_duration=clip_duration)
        return results

def process_bilibili_climax(
    bvids: str | List[str],
    clip_duration: float = 20.0,
    output_dir: str = "output",
    temp_dir: str = "temp",
    ffmpeg_bin: str = "ffmpeg",
) -> Dict[str, Optional[Path]]:
    """
    外部调用入口。
    """
    if isinstance(bvids, str):
        bvid_list = [bvids]
    else:
        bvid_list = list(bvids)

    clipper = BilibiliClimaxClipper(output_dir=output_dir, temp_dir=temp_dir, ffmpeg_bin=ffmpeg_bin)
    return clipper.process_bvids(bvid_list, clip_duration=clip_duration)


if __name__ == "__main__":
    test_bvids = ["BV16rc2exEne"]
    results = process_bilibili_climax(test_bvids, clip_duration=10.0)
    print("Results:", results)
