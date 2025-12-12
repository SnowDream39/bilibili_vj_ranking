# utils/climax_clipper.py
from typing import Tuple, List
import librosa
import numpy as np

def _normalize(x: np.ndarray) -> np.ndarray:
    """将数组线性归一化到 [0, 1] 区间。"""
    x = x.astype(float)
    min_v = x.min()
    max_v = x.max()
    if max_v - min_v < 1e-8:
        return np.zeros_like(x)
    return (x - min_v) / (max_v - min_v)


def _compute_vocaloid_activity(
    y: np.ndarray,
    sr: int,
    hop_length: int = 512
) -> np.ndarray:
    """
    检测VOCALOID/合成人声活跃度。
    
    策略：
    1. 频谱通量（Spectral Flux）：旋律变化越快，得分越高
    2. 频谱平坦度（Spectral Flatness）：越低说明有明确音高（不是噪音/纯打击乐）
    3. 中频段能量：V家人声主要集中在中频
    
    Returns:
        np.ndarray: 每帧的VOCALOID活跃度 [0, 1]
    """
    # 1. 计算短时傅里叶变换
    stft = librosa.stft(y, hop_length=hop_length)
    mag = np.abs(stft)
    flux = np.sum(np.diff(mag, axis=1)**2, axis=0)
    flux = np.concatenate([[0], flux])  # 补齐长度
    flux_n = _normalize(flux)
    flatness = librosa.feature.spectral_flatness(y=y, hop_length=hop_length)[0]
    # 反转：平坦度低 -> 得分高
    tonal_score = 1.0 - flatness
    tonal_n = _normalize(tonal_score)
    
    freqs = librosa.fft_frequencies(sr=sr)
    mid_freq_mask = (freqs >= 500) & (freqs <= 4000)
    mid_energy = np.mean(mag[mid_freq_mask, :], axis=0)
    mid_n = _normalize(mid_energy)
    
    vocaloid_score = 0.2 * flux_n + 0.5 * tonal_n + 0.3 * mid_n
    
    return vocaloid_score


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

    # 1. 基础特征
    rms = librosa.feature.rms(y=y, hop_length=hop_length)[0]
    onset_env = librosa.onset.onset_strength(y=y, sr=sr, hop_length=hop_length)
    
    # 2. VOCALOID活跃度
    vocaloid_activity = _compute_vocaloid_activity(y, sr, hop_length)

    # 对齐长度
    min_len = min(len(rms), len(onset_env), len(vocaloid_activity))
    rms = rms[:min_len]
    onset_env = onset_env[:min_len]
    vocaloid_activity = vocaloid_activity[:min_len]

    rms_n = _normalize(rms)
    onset_n = _normalize(onset_env)
    vocaloid_n = _normalize(vocaloid_activity)

    # 3. 块级重复度
    rep_block, block_times = _compute_block_chroma_repetition(y, sr, block_sec=1.0)
    if len(rep_block) == 0:
        rep_block = np.zeros(1)
        block_times = np.array([0.5])

    frame_idx = np.arange(min_len)
    frame_times = librosa.frames_to_time(frame_idx, sr=sr, hop_length=hop_length)
    rep_frame = np.interp(frame_times, block_times, rep_block)
    rep_n = _normalize(rep_frame)

    # 4. 综合打分
    # VOCALOID旋律性 > 响度 > 节奏 > 重复度
    alpha_vocaloid = 0.30
    alpha_rms = 0.20
    alpha_onset = 0.25
    alpha_rep = 0.25
    
    combined_score = (
        alpha_vocaloid * vocaloid_n +
        alpha_rms * rms_n +
        alpha_onset * onset_n +
        alpha_rep * rep_n
    )

    frames_per_sec = sr / hop_length
    window_frames = int(clip_duration * frames_per_sec)
    duration = len(y) / sr

    if window_frames <= 1 or window_frames >= len(combined_score):
        return 0.0, min(float(duration), clip_duration)

    # 5. 滑动窗口打分
    kernel = np.ones(window_frames, dtype=float)
    window_scores = np.convolve(combined_score, kernel, mode="valid")
    start_frame_idx = np.arange(len(window_scores))
    start_times = librosa.frames_to_time(start_frame_idx, sr=sr, hop_length=hop_length)

    # 6. 位置约束
    start_margin = 5.0
    end_margin = 5.0
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

    # 7. 节拍对齐
    search_back = 1.0
    search_forward = 1.0

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
        near_beats = candidate_beats[candidate_beats >= rough_start - 0.05]
        if len(near_beats) == 0:
            near_beats = candidate_beats
        final_start = float(near_beats[np.argmin(np.abs(near_beats - rough_start))])
    else:
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
    final_start = max(0.0, final_start - 0.03)
    final_end = min(float(duration), final_start + clip_duration)

    return float(final_start), float(final_end)
