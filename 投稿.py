# -*- coding: utf-8 -*-
"""
自动找到 daily_video 中最新一期的竖屏视频。
账号 / 分区 / 标签 / 简介 等从 config/daily_upload.yaml 读取。
"""
import re
import json
import yaml
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, List

CONFIG_DIR = Path("config")
UPLOAD_CONFIG_PATH = CONFIG_DIR / "daily_upload.yaml"

def load_upload_config() -> Dict:
    if not UPLOAD_CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"未找到投稿配置文件: {UPLOAD_CONFIG_PATH}\n"
            f"请先创建 config/daily_upload.yaml。"
        )
    with open(UPLOAD_CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg

def cn_date_from_yyyymmdd(yyyymmdd: str) -> str:
    dt = datetime.strptime(yyyymmdd, "%Y%m%d")
    return f"{dt.year}年{dt.month}月{dt.day}日"

def find_latest_issue_video(base_dir: Path) -> Tuple[Path, Path, int, str]:
    if not base_dir.exists():
        raise FileNotFoundError(f"日刊视频目录不存在: {base_dir}")

    final_videos: List[Tuple[Path, int, str]] = []

    for p in base_dir.glob("*.mp4"):
        stem = p.stem
        if stem.startswith("tmp_"):
            continue

        m = re.match(r"^(\d+)_((?:19|20)\d{6})$", stem)
        if not m:
            continue

        issue = int(m.group(1))
        date_str = m.group(2)
        final_videos.append((p, issue, date_str))

    if not final_videos:
        raise RuntimeError(f"在 {base_dir} 中没有找到形如 {{期数}}_{{日期}}.mp4 的成品视频，")

    final_videos.sort(key=lambda x: (x[2], x[1]))
    video_path, issue, yyyymmdd = final_videos[-1]

    cover_path = base_dir / f"{issue}_{yyyymmdd}_cover.jpg"
    if not cover_path.exists():
        raise FileNotFoundError(
            f"找到了最新一期视频 {video_path}，但对应封面不存在: {cover_path}"
        )

    print("检测到最新一期：")
    print(f"  期数: {issue}")
    print(f"  日期: {yyyymmdd} ({cn_date_from_yyyymmdd(yyyymmdd)})")
    print(f"  视频: {video_path}")
    print(f"  封面: {cover_path}")
    return video_path, cover_path, issue, yyyymmdd

def build_title(issue: int, yyyymmdd: str) -> str:
    return f"日刊虚拟歌手外语排行榜#{issue} {cn_date_from_yyyymmdd(yyyymmdd)}"

def build_dynamic(issue: int, yyyymmdd: str) -> str:
    return f"日刊虚拟歌手外语排行榜#{issue} {cn_date_from_yyyymmdd(yyyymmdd)}"

def run_biliup_upload(
    biliup_exe: str,
    video: Path,
    cover: Path,
    title: str,
    desc: str,
    tags: List[str],
    topic_id: int,
    tid: int,
    dynamic: str,
) -> str:
    """
    使用 biliup CLI 投稿。
    biliup_exe 从配置中读取，可以是 "biliup" 或 "C:/xxx/biliup.exe"
    """
    if not video.exists():
        raise FileNotFoundError(f"视频不存在: {video}")
    if not cover.exists():
        raise FileNotFoundError(f"封面不存在: {cover}")

    tag_str = ",".join(tags)
    cmd = [
        biliup_exe,
        "--user-cookie", "config/cookies.json",
        "upload",
        str(video),
        "--tid",
        str(tid),
        "--cover",
        str(cover),
        "--title",
        title,
        "--desc",
        desc,
        "--tag",
        tag_str,
        "--extra-fields",
        json.dumps({"topic_id": int(topic_id)}, ensure_ascii=False),
        "--dynamic",
        dynamic,
        "--copyright",
        "1", 
        "--no-reprint",
        "0",
    ]

    print("\n开始投稿 B 站：")
    print(f"  标题：{title}")
    print(f"  分区：音乐区 -> VOCALOID·UTAU (tid={tid})")
    print(f"  标签：{tag_str}")
    print(f"  动态：{dynamic}")
    print("  命令：", " ".join(cmd))

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        raise FileNotFoundError()

    out = (proc.stdout or "") + "\n" + (proc.stderr or "")
    print(out)

    for log_name in ("download.log", "ds_update.log", "upload.log", "qrcode.png"):
        p = Path(log_name)
        if p.exists():
            try:
                p.unlink()
            except OSError:
                pass

    if proc.returncode != 0:
        print("\n投稿失败，biliup 输出如下：")
        print(out)
        raise RuntimeError(f"biliup upload 失败（returncode={proc.returncode}）")

    return out

def extract_bvid(text: str) -> str:
    m = re.search(r"(BV[0-9A-Za-z]{10})", text)
    if not m:
        raise RuntimeError("未能从 biliup 输出中找到 BV 号，请检查上传输出。")
    return m.group(1)

def main() -> None:
    cfg = load_upload_config()

    # daily_video 目录
    daily_dir = Path(
        (cfg.get("daily_video") or {}).get("dir")
    )

    # biliup 可执行文件
    biliup_exe = (cfg.get("biliup") or {}).get("exe", "biliup")

    # 投稿参数
    post_cfg = cfg.get("post") or {}
    tid = int(post_cfg.get("tid"))
    tags = post_cfg.get("tags")
    desc = post_cfg.get("desc")
    topic_id = post_cfg.get("topic_id")

    # 1. 找最新一期
    video_path, cover_path, issue, yyyymmdd = find_latest_issue_video(daily_dir)

    # 2. 标题 & 动态
    title = build_title(issue, yyyymmdd)
    dynamic = build_dynamic(issue, yyyymmdd)
    # 3. 投稿
    out = run_biliup_upload(
        biliup_exe=biliup_exe,  
        video=video_path,
        cover=cover_path,
        title=title,
        desc=desc,
        tags=tags,
        topic_id=topic_id,
        tid=tid,
        dynamic=dynamic,
    )

    # 4. 提取 BV
    bvid = extract_bvid(out)
    print(f"\n投稿成功，BV 号：{bvid}")

    print("\n全部完成")


if __name__ == "__main__":
    main()
