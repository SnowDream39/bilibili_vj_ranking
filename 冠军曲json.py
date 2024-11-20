import pandas as pd
import json

# 读取Excel文件
file_path = "周刊/总榜/rank_1_summary.xlsx"
df = pd.read_excel(file_path)

# 假设Excel文件的结构是：歌曲名称、总分、日期等

# 转换为字典形式
chart_data = {
    "title": {
        "text": "旧周刊(#1~#612)总分数据",
        "subtext": "图示",
        "subtextStyle": {
            "color": "#333"
        }
    },
    "tooltip": {
        "trigger": "axis",
        "axisPointer": {
            "type": "cross",
            "animation": False
        }
    },
    "toolbox": {
        "show": True,
        "feature": {
            "dataZoom": {
                "yAxisIndex": "none"
            },
            "dataView": {
                "readOnly": True
            },
            "magicType": {
                "type": ["line", "bar"]
            },
            "restore": {},
            "saveAsImage": {
                "excludeComponents": ["toolbox", "dataZoom"]
            }
        }
    },
    "dataZoom": [
        {
            "type": "inside",
            "xAxisIndex": [0],
            "startValue": 0,
            "end": 100
        },
        {
            "show": True,
            "xAxisIndex": [0],
            "type": "slider",
            "start": 0,
            "end": 100
        }
    ],
    "legend": {
        "data": ["总分"]
    },
    "xAxis": {
        "data": df['name'].tolist()  # 读取Excel文件中的歌曲名称
    },
    "yAxis": [
        {
            "type": "value"
        }
    ],
    "series": [
        {
            "name": "总分",
            "type": "line",
            "data": df['point'].tolist()  # 读取Excel文件中的总分数据
        }
    ]
}

# 输出为JSON格式
json_data = json.dumps(chart_data, ensure_ascii=False, indent=4)

# 保存为json文件
with open("output.json", "w", encoding="utf-8") as f:
    f.write(json_data)

print("JSON文件已保存")
