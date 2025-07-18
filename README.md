# 术力口数据库

本项目用于术力口数据库的数据和排行的原始文件处理，包含数据抓取、处理以及排行榜生成，涵盖日刊、周刊和月刊等不同类别。以下是具体操作步骤。

### 1. 数据获取

每天0点准时运行以下脚本：
- `抓取数据.py`
- `抓取新曲数据.py`

数据会以`yyyyMMdd`格式存储至：
- `数据/` （非新曲部分）
- `新曲数据/`（新曲部分）

### 2. 数据处理

#### 2.1 计算数据
运行 `计算数据.py` 脚本，该脚本会加载最新的数据文件，计算当日的排行。处理后的数据将存放于：

- `差异/非新曲/`（非新曲部分）
- `差异/新曲/`（新曲部分）

#### 2.2 处理新曲差异文件
新曲差异文件需人工筛选和修改。重点包括：
- `name`： 修正为通称曲名
- `author` ：修正为通称作者
- `title` &`uploader` ：保持视频标题和UP主昵称不变
- `copyright`：若为转载但标记为自制，则修改为 `3`
- 标注使用引擎（如`VOCALOID`）、歌手（如`初音未来`）及视频类型（如`原创`）等

修改完成后，直接覆盖原文件保存。

### 3. 日刊

#### 3.1 生成总榜
- 运行 `合并.py`，程序会合并各项数据并输出至 `差异/合并曲目/`。

#### 3.2 生成新曲排行榜
- 运行 `新曲排行榜.py`，程序会按排行逻辑输出至 `新曲榜/` 目录。

### 4. 周刊与月刊

周刊和月刊依赖日常数据更新，生成方式与日刊类似。

#### 4.1 周刊
- 运行 `周刊.py` 并输入所需日期，程序会生成：
  - `周刊/总榜/`（总榜数据）
  - `周刊/新曲榜/`（新曲数据）

#### 4.2 月刊
- 运行 `月刊.py` 并输入相关日期，程序会生成：
  - `月刊/总榜/`（总榜数据）
  - `月刊/新曲榜/`（新曲数据）

### 5. 其他工具

- `百万播放播报.py`：统计期间内播放量突破百万的视频。
- `抓取特殊数据.py`：制作特刊时使用的脚本。
- `计算总榜数据.py`：生成对视频总数据进行排名的排行榜。
