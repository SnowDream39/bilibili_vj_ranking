import os
from openpyxl import load_workbook

old_value = "GEMN(yasai31)"  # 要查找的字符串
new_value = "FATAL(yasai31)(Short)"  # 替换成的字符串

def process_excel_file(file_path):
    try:
        wb = load_workbook(file_path)
        modified = False 

        for ws in wb.worksheets:
            for row in ws.iter_rows(min_row=ws.min_row, max_row=ws.max_row,
                                    min_col=ws.min_column, max_col=ws.max_column):
                for cell in row:
                    if cell.value == old_value:
                        print(f"文件 {file_path} 工作表 {ws.title} 单元格 {cell.coordinate} 内容为 '{old_value}'，替换为 '{new_value}'")
                        cell.value = new_value
                        modified = True

        if modified:
            wb.save(file_path)
        else:
            print(f"文件 {file_path} 中没有找到值为 '{old_value}' 的单元格")
    except Exception as e:
        print(f"处理文件 {file_path} 时出错：{e}")

def traverse_folder(root_folder):
    for dirpath, dirnames, filenames in os.walk(root_folder):
        for filename in filenames:
            if filename.lower().endswith(".xlsx"):
                file_path = os.path.join(dirpath, filename)
                process_excel_file(file_path)

if __name__ == "__main__":
    root_folder = os.path.abspath(os.path.dirname(__file__))
    traverse_folder(root_folder)
