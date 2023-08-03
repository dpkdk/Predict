import pandas


class ExcelReader:

    def read_excel(self, path, sheet_name):
        result_data = []
        data_frame = pandas.read_excel(path, sheet_name=sheet_name, header=None)
        for i in range(len(data_frame)):
            if i == 0:
                field_list = data_frame.iloc[0].values.tolist()  # 各字段名称
            else:
                result_data_item = {}
                for j in range(len(field_list)):
                    result_data_item[field_list[j]] = data_frame.iloc[i, j]
                result_data.append(result_data_item)
        return result_data

