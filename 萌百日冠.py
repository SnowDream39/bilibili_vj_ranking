
import pandas as pd
color = {
  "LEON":"#F7E8BD",
  "LOLA":"#7D0552",
  "MIRIAM":"#516B82",
  "MEIKO":"#D80000",
  "KAITO":"#0000FF", 
  "Sweet Ann": "#FFE391",
  "初音未来": "#39C5BB",
  "弱音白": "#DCDCDC",
  "镜音铃": "#FF8800",
  "镜音连": "#FFF000",
  "Prima": "#B92E66",
  "巡音流歌": "#FFB6C1",
  "神威乐步": "#9900FF",
  "GUMI": "#CCFF00",
  "SONiKA": "#243D33",
  "冰山清辉": "#1D1F2F",
  "歌爱雪": "#F811DE",
  "miki": "#FF7070",
  "BIG AL": "#D40000",
  "Lily": "#FFCC00",
  "VY1": "#3C0036",
  "Gachapoid": "#0C9A06",
  "猫村伊吕波": "#FC79A7",
  "歌手音Piko": "#4ACDBF",
  "VY2": "#4D0000",
  "Akikoloid酱": "#5481DE",
  "Mew": "#221815",
  "SeeU": "#FFAE35",
  "兔眠莉音": "#F4A89D",
  "Oliver": "#F3E7BF",
  "CUL": "#D41A1F",
  "结月缘": "#800080",
  "Clara": "#587078",
  "Bruno": "#BF4E43",
  "IA": "#F5EDED",
  "苍姬拉碧斯": "#5BD1D5",
  "洛天依": "#66CCFF",
  "Galaco": "#FAE6FA",
  "MAYU": "#E452A7",
  "AVANNA": "#EEE8AA",
  "KYO": "#224267",
  "WIL": "#2A221F",
  "YUU": "#D7B3CF",
  "言和": "#00FFCC",
  "YOHIOloid": "#F6E9BD",
  "MAIKA": "#77C6EC",
  "kokone": "#FBB7A4",
  "杏音": "#FFC369",
  "鸟音": "#FFC370",
  "V flower": "#996699",
  "东北俊子": "#1C3A46",
  "Rana": "#DD315D",
  "Chika": "#FFF0FB",
  "心华": "#EE82EE",
  "乐正绫": "#EE0000",
  "Sachiko": "#BC255A",
  "Ruby": "#D86756",
  "DAINA": "#0764B0",
  "DEX": "#D9DEEC",
  "Fukase": "#FF0033",
  "星尘": "#9999FF",
  "音街鳗": "#0000AA",
  "UNI": "#F56C98",
  "乐正龙牙": "#006666",
  "LUMi": "#F8FDFF",
  "绁星灯": "#FFBA70",
  "徵羽摩柯": "#0080FF",
  "墨清弦": "#FFFF00",
  "樱乃空": "#FFE9D8",
  "鸣花姬": "#F0B4BC",
  "鸣花尊": "#AFAFEF",
  "Po-uta": "#739330",
  "战音Lorra": "#FFFFFF",
  "Ken": "#ACD291",
  "呗音Uta": "#36015A",
  "重音Teto": "#D93A49",
  "桃音Momo": "#FF9FCF",
  "欲音Ruko": "#000080",
  "波音律": "#FF2D51",
  "健音帝": "#FF8080",
  "雪歌Yufu": "#F2F2F2",
  "实谷Nana": "#74BC9F",
  "渗音Kakoi": "#D2E9A4",
  "愛野Hate": "#99669A",
  "白鐘Hiyori": "#DEB887",
  "春歌Nana": "#FB73CA",
  "樱歌Miko": "#FFC0CB",
  "海歌Shin": "#FFA500",
  "空音Rana": "#EF6C00",
  "破坏音Maiko": "#EC66FF",
  "松田Ppoiyo": "#07C7CF",
  "薪宮風季": "#4169E1",
  "椎音Ama": "#DC143C",
  "朱音稻荷": "#CC0000",
  "彼音Izumu": "#E20000",
  "飴音Wamea": "#FBCCA7",
  "雨歌Eru": "#008080",
  "廻音Shuu": "#EB8938",
  "暗音Renri": "#B060BC",
  "歌幡Meiji": "#9773BF",
  "暗鸣Nyui": "#D8B69E",
  "戯白Merry": "#FF7405",
  "逆音Cecil": "#EDAE2F",
  "夕歌ユウマ": "#526B65",
  "Number Bronze": "#DCEEDB",
  "东北伊达子": "#E1E6F9",
  "东北切蒲英": "#7F3D61",
  "小感冒": "#573A2B",
  "剧药": "#68483B",
  "旭音Ema": "#008E94",
  "足立零": "#ED8D2D",
  "油库里": "#000001",
  "佐藤莎莎拉": "#FFEFF2",
  "铃木梓梓弥":"#7A80A4",
  "ONE":"#F5EEAF",
  "可不":"#4D79FF",
  "星界":"#7933FF",
  "知声":"#F1971C",
  "里命":"#E51500",
  "POPY":"#FA006E",
  "ROSE":"#5050D2",
  "狐子":"#CDE6F2",
  "羽累":"#3CD705",
  "月读爱":"#CB8773",
  "弦卷真纪":"#FFE791",
  "琴叶茜":"#FFC0C0",
  "琴叶葵":"#E1F0F7",
  "京町精华":"#6FBA44",
  "追傩酱":"#F4C3D7",
  "永夜Minus":"#613C8A",
  "小春六花":"#ACA8B2",
  "夏色花梨":"#A6727F",
  "花隈千冬":"#A1D6B7",
  "Mai":"#ED6772",
  "奕夕":"#cc164b",
  "绮萱":"#84D0D0",
}

def generate_wikitable(input_file, output_file):
    # 读取Excel文件
    df = pd.read_excel(input_file)

    # 打开输出文件
    with open(output_file, 'w', encoding='utf-8') as f:
        # 写入wikitable的表头部分
        f.write('{| class="wikitable sortable" style="text-align:center;"\n')
        f.write('|-\n')
        f.write('! #期数 !! 冠军歌曲 !! 歌姬 !! P主 !! 得点(pts)\n')
        
        # 遍历DataFrame并生成每一行的内容
        for index, row in df.iterrows():
            f.write('|-\n')
            if "、" in row["vocal"]:
                vocals = row["vocal"].split("、")
                color_values = [color.get(v) for v in vocals]
                if(len(color_values)>2):
                    f.write(f'| {row["edition"]} || [[{row["name"]}]] || style="background:#777777"| 复数 || {row["author"]} || {row["point"]}\n')
                else:
                    f.write(f'| {row["edition"]} || [[{row["name"]}]] || style="background: linear-gradient(to right, {color_values[0]} 50%, {color_values[1]} 50%);"| {vocals[0]}、{vocals[1]} || {row["author"]} || {row["point"]}\n')
            else:
                color_values = [color.get(row["vocal"])]
                f.write(f'| {row["edition"]} || [[{row["name"]}]] || style="background:{color_values[0]}"|{row["vocal"]} || {row["author"]} || {row["point"]}\n')
            
        
        # 写入表格的结束标志
        f.write('|}\n')

    print(f"Wikitable successfully saved to {output_file}")

# 输入Excel文件路径
input_file = r"E:\Programming\python\bilibili日V周刊\差异\合并表格\rank_1_summary.xlsx"

# 输出txt文件路径
output_file = r'E:\Programming\python\bilibili日V周刊\测试内容\output_week.txt'

# 生成wikitable
generate_wikitable(input_file, output_file)
