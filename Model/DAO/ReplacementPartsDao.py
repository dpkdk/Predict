import pandas as pd

from Model.DA.AircraftRegistration import AircraftRegistration
from Model.DA.ReplacementParts import ReplacementParts
from Utils.ExcelReader import ExcelReader
from Utils.tools import *


class ReplacementPartsDao:
    def __init__(self, db):
        self.rp = ReplacementParts(db)
        self.ar = AircraftRegistration(db)

    # 读20220630-20230701发料.xlsx
    def read_replacement_parts(self):
        path = "Files/Tables/20220701-20230630拆换件信息.xlsx"
        sheet_name = "Result2 (2)"
        er = ExcelReader()
        result_data = er.read_excel(path, sheet_name)
        return result_data

    # 拆换件信息对应写入数据库
    def write_replacement_parts(self):
        dataframe = self.read_replacement_parts()  # dataframe[0]为要插入的一条数据
        for data in dataframe:
            print(data)
            if self.rp.select_from_remove_date_removed_part_serial(data) == ():  # 唯一性检验 拆下日期+拆下序号
                if data['机号'] is None:
                    data['机号'] = 'NULL'
                registration_id = self.ar.select_id_from_registration(data['机号'])
                if registration_id != ():  # 在已知列表则写入表，否则不写入
                    data['机号'] = registration_id
                    data['实物拆下日期'] = datetime_to_string(data['实物拆下日期'])
                    self.rp.insert_table(data)

    # 统计各机号发生周转件拆换总次数,按次数降序(inner直接count版)-->可直接得到需要机型列表
    def get_total_replacement_count_by_all_registration_inner(self):
        registration_list = []  # 存入机号id
        result = self.rp.select_registration_count_inner()
        for res in result:
            registration_list.append(res[0])
            print(f"id={res[0]}，机号{res[1]}拆换次数为{res[2]}")
        return registration_list

    # 统计每个机号发生周转件拆换总次数**循环单个得到详情
    def get_total_replacement_count_by_all_registration(self):
        date_list = generate_dates("2022-06-30", "2023-07-01")
        registration_list = self.get_total_replacement_count_by_all_registration_inner()
        for registration_item in registration_list:  # 每一个机号id
            replacement_list = self.get_total_replacement_count_by_registration(registration_item)
            print(date_list)
            print(replacement_list)

    # 统计指定机号发生周转件拆换总次数
    def get_total_replacement_count_by_registration(self, registration_id):
        replacement_result = self.rp.select_from_registration_id_and_part_type_inner(registration_id)  # 由机号id查询得到具体拆换条目
        replacement_info = [[] for _ in range(2)]
        for res in replacement_result:  # 列出拆换时间
            replacement_info[0].append(res[2])
            replacement_info[1].append(res[3])
            # print(f"id={res[0]}, {res[1]}在{res[2]}发生{res[3]}次拆换")
        replacement_list = generate_replacement_date_list("2022-06-30", "2023-07-01", replacement_info)
        '''可以写入数据库'''
        return replacement_list

    # 统计每个机号发生周转件拆换总次数(精确到件号)**循环单个得到详情
    def get_part_replacement_count_by_all_registration(self):
        registration_list = self.get_total_replacement_count_by_all_registration_inner()
        for registration_item in registration_list:  # 每一个机号id
            print(registration_item)
            self.get_part_replacement_count_by_registration(registration_item)


    # 统计指定机号发生周转件拆换总次数(精确到件号)
    def get_part_replacement_count_by_registration(self, registration_id):
        part_result = self.rp.select_all_part_number_from_registration_id(registration_id)  # 由机号id查询得到具体件号拆换条目
        for part in part_result:  # 每一个件号
            part_number = part[1]
            replacement_list = self.get_part_replacement_by_part_number(registration_id, part_number)
            '''可以写入数据库'''
            print("件号", part_number)
            print("按日", generate_dates("2022-06-30", "2023-07-01"))
            print("按日", replacement_list)
            key_lst, lst = aggregate_data_by_month("2022-06-30", "2023-07-01", replacement_list)
            print("按月", key_lst)
            print("按月", lst)

    # 返回指定一个机号中的一个件号发生拆换时间列表
    def get_part_replacement_by_part_number(self, registration_id, part_number):
        replacement_result = self.rp.select_date_from_registration_id_part_number(registration_id, part_number)  # 由机号id查询得到具体拆换条目,包括件号
        replacement_info = [[] for _ in range(2)]
        for res in replacement_result:  # 列出拆换时间
            replacement_info[0].append(res[1])
            replacement_info[1].append(res[2])
            # print(f"id={res[0]}, {res[1]}在{res[2]}发生{res[3]}次拆换")
        replacement_list = generate_replacement_date_list("2022-06-30", "2023-07-01", replacement_info)
        return replacement_list


    # 获取所有零件拆换信息
    def get_replacement_count_part(self):
        part_number_result = self.rp.select_part_number_group()
        for part_number in part_number_result:
            replacement_list = self.get_replacement_by_part_number(part_number[0])  # part_number[0]是partnumber[1]是count
            '''可以写入数据库'''
            print("件号", part_number)
            print("按日", generate_dates("2022-06-30", "2023-07-01"))
            print("按日", replacement_list)
            key_lst, lst = aggregate_data_by_month("2022-06-30", "2023-07-01", replacement_list)
            print("按月", key_lst)
            print("按月", lst)


    # 获取一个零件拆换发生时间
    def get_replacement_by_part_number(self, part_number):
        replacement_result = self.rp.select_date_count_by_part_number(part_number)  # 由件号名称查询得到具体拆换条目（按日期分类
        replacement_info = [[] for _ in range(2)]
        for res in replacement_result:  # 列出拆换时间
            replacement_info[0].append(res[1])
            replacement_info[1].append(res[2])
            # print(f"id={res[0]}, {res[1]}在{res[2]}发生{res[3]}次拆换")
        replacement_list = generate_replacement_date_list("2022-06-30", "2023-07-01", replacement_info)
        return replacement_list


    # 统计各件号发生周转件拆换总次数,按次数降序-->可直接得到需要机型列表
    def get_total_replacement_count_by_all_part_number(self):
        result = self.rp.select_part_number_group()
        for res in result:
            print(f"件号{res[0]}拆换次数为{res[1]}")

