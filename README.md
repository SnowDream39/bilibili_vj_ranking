# 日刊哔哩哔哩外语排行榜

## 概述

本项目用于计算并生成日刊哔哩哔哩外语排行榜。项目包含多个 Python 脚本来抓取数据、计算差异、以及生成排行榜。请按以下步骤操作。

## 日刊功能说明

### 数据获取

1. **每日数据抓取**

   - 为确保数据正常获取，请在每日0点准时启动 `抓取数据.py`和`抓取新曲数据.py`。并将数据分别输出到以下路径：
     - `数据/{爬取日期}.xlsx`
     - `新曲数据/新曲{爬取日期}.xlsx`

### 数据处理

2. **计算数据**
   - 运行 `计算数据.py`。该脚本需要提供以下4个文件路径：
     - 今日新曲数据
     - 昨日新曲数据
     - 今日非新曲数据
     - 昨日非新曲数据
   
   - 脚本将生成数据日增文件和新曲、非新曲差异文件。输出文件路径如下：
     - `差异/新曲/新曲xxxx与新曲xxxx.xlsx`
     - `差异/非新曲/xxxx与xxxx.xlsx`
   
3. **处理新曲差异文件**

   - 对于新曲差异文件，需要人工筛选合适的视频，并进行以下修改：
     - 修改通称曲名（`name` 一栏）
     - 修改通称作者（`author` 一栏）
     - 保持 `title` 为视频标题
     - 保持 `uploader` 为 UP 主昵称
     - 对于转载视频而被UP主设为自制视频的, 请将`copyright`一值改成3
     
   - 修改完成后，请原地保存文件。

### 排行榜生成

4. **制作新曲排行榜**

   - 打开 `新曲排行榜.py`，将 `today` 值设置为需要的日期值。
   - 输入刚才修改的差异新曲文件路径。
   - 运行程序，排行榜将输出到 `新曲榜/xxxx`。

5. **制作总榜**

   - 打开 `合并.py`，输入非新曲差异文件和修改过的新曲差异文件的路径。
   - 运行程序，总榜数据将输出到 `差异/合并曲目/xxxx`。
   - 对于完整的日增文件，建议将文件名修改为 `yyyymmdd与yyyymmdd.xlsx` 的形式，然后复制到完整日文件夹里。

## 文件说明

- `计算公式.txt`：排行榜计算公式的详细说明
- `抓取数据.py`：自动抓取数据
- `抓取新曲数据.py`：抓取新曲数据
- `计算数据.py`：计算数据日增及差异
- `新曲排行榜.py`：生成新曲排行榜
- `合并.py`：生成总榜

## 其他模块
- `月榜.py`：制作月刊，使用方法与日刊类似
- `计算总榜数据.py`：生成需要对视频总数据进行排名的排行榜
- `简易算分.py`：输入视频数据，得到各项得分
  
## 许可证

本项目采用 [MIT 许可证](LICENSE) 进行许可。
