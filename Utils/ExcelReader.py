
import pandas as pd


class ExcelReader:

    def read_excel(self, path, sheet_name):
        result_data = []
        data_frame = pd.read_excel(path, sheet_name=sheet_name, header=None)
        for i in range(len(data_frame)):
            if i == 0:  # 取出各字段名称存入field_list
                field_list = data_frame.iloc[0].values.tolist()  # 各字段名称
            else:
                result_data_item = {}
                for j in range(len(field_list)):
                    if pd.isna(data_frame.iloc[i, j]):  # 将excel中的NaN转为sql可识别的Null
                        data_frame.iloc[i, j] = None
                    result_data_item[field_list[j]] = data_frame.iloc[i, j]
                result_data.append(result_data_item)
        return result_data

    def read_txt(self, path):
        result_data = []
        with open(path, 'r', encoding='utf-8') as file:
            field_names = file.readline().strip().split(' ')

            # 逐行读取文件内容
            for line in file:
                # 去除行首尾的空格和换行符
                line = line.strip()
                # 以空格分割行内容
                line_parts = line.split(' ')
                # 构建字典
                result_data_item = {}
                for i, field_value in enumerate(line_parts):
                    result_data_item[field_names[i]] = field_value
                # 将字典添加到列表中
                result_data.append(result_data_item)
        return result_data

