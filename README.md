# 术力口数据库

## 项目概述

本项目旨在计算和生成与术力口数据库相关的内容。项目包含多个 Python 脚本，用于数据抓取、处理和生成排行榜。内容分为日刊、周刊、月刊等不同部分。请按照以下步骤操作，以确保数据的准确性和完整性。

## 使用指南

### 1. 数据获取

#### 1.1 每日数据抓取
- **操作步骤：**
  - 每天0点准时运行以下脚本：
    - `抓取数据.py`
    - `抓取新曲数据.py`
- **输出路径：**
  - 抓取到的数据将分别保存在以下文件中：
    - `数据/`
    - `新曲数据/`

### 2. 数据处理

#### 2.1 计算数据
- **操作步骤：**
  - 运行 `计算数据.py` 脚本。
  - 该脚本会自动加载今日和昨日的数据文件，并进行计算。
- **输出结果：**
  - 脚本将生成以下文件：
    - `差异/新曲/`
    - `差异/非新曲/`

#### 2.2 处理新曲差异文件
- **注意事项：**
  - 对于新曲差异文件，需要进行人工筛选和修改。请确保以下项被更新：
    - 修改 `name` 列为通称曲名
    - 修改 `author` 列为通称作者
    - 保持 `title` 为视频标题
    - 保持 `uploader` 为 UP 主昵称
    - 对于转载视频被设为自制的情况，将 `copyright` 值改为 3
    - 标注歌曲所用引擎、歌手及视频类型（例如：原创、翻唱、本家重置）
- **保存文件：**
  - 修改完成后，请在原文件上保存更改。

### 3. 日刊

#### 3.1 新曲排行榜
- **操作步骤：**
  - 打开 `新曲排行榜.py`，并将 `today` 值设置为所需日期。
  - 运行程序后，新曲排行榜将输出至 `新曲榜/`。

#### 3.2 总榜生成
- **操作步骤：**
  - 打开 `合并.py`，输入待制作的日刊日期。
  - 运行程序后，总榜数据将输出至 `差异/合并曲目/`。

### 4. 周刊与月刊

周刊和月刊的生成依赖于日常数据更新，操作步骤与日刊类似。

#### 4.1 周刊
- **操作步骤：**
  - 打开 `周刊.py`，输入相关日期以生成总榜和新曲榜。
- **输出路径：**
  - 总榜保存至 `周刊/总榜/`
  - 新曲榜保存至 `周刊/新曲榜/`

#### 4.2 月刊
- **操作步骤：**
  - 打开 `月刊.py`，输入相关日期以生成总榜和新曲榜。
- **输出路径：**
  - 总榜保存至 `月刊/总榜/`
  - 新曲榜保存至 `月刊/新曲榜/`

### 5. 其他工具

- `百万播放播报.py`：生成期间内突破百万播放的视频列表。
- `抄榜.py`：用于日刊、周刊和月刊的抄榜。
- `特殊-抓取数据.py`：制作特刊时使用的脚本。
- `计算总榜数据.py`：生成对视频总数据进行排名的排行榜。
- `简易算分.py`：根据输入的视频数据计算得分。
