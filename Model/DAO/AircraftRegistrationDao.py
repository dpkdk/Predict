import pandas as pd

from Model.DA.AircraftRegistration import AircraftRegistration
from Model.DA.Base import Base
from Utils.ExcelReader import ExcelReader
from Utils.tools import *


class AircraftRegistrationDao:
    def __init__(self, db):
        self.ar = AircraftRegistration(db)
        self.base = Base(db)


    # 读南航飞机布局.xlsx
    def read_aircraftregistration(self):
        path = "Files/Tables/南航飞机布局.xlsx"
        sheet_name = "Sheet1"
        er = ExcelReader()
        result_data = er.read_excel(path, sheet_name)
        return result_data

    # 南航飞机布局.xlsx写入数据库,关联基地表（一条对应一个基地id
    def write_aircraftregistration(self):
        dataframe = self.read_aircraftregistration()  # dataframe[0]为要插入的一条数据
        for data in dataframe:
            print(data)
            if self.ar.select_from_registration(data) == ():  # 唯一性检验 机号
                base_id = self.base.select_id_from_city(data['执管单位'])  # 连表
                self.ar.insert_table(data, base_id)

    # 各基地执管飞机架数计算（机号
    def count_num_by_registration(self):
        pass


    # 各基地执管机型计算（机型
    def count_num_by_aircrafttype(self):
        pass

