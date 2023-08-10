import pandas as pd

from Model.DA.AircraftRegistration import AircraftRegistration
from Model.DA.Issuance import Issuance
from Model.DA.Warehouse import Warehouse
from Utils.ExcelReader import ExcelReader
from Utils.tools import *


class IssuanceDao:
    def __init__(self, db):
        self.issuance = Issuance(db)
        self.ar = AircraftRegistration(db)
        self.warehouse = Warehouse(db)


    # 读20220630-20230701发料.xlsx
    def read_issuance(self):
        path = "Files/Tables/20220630-20230701发料.xlsx"
        sheet_name = "Sheet1"
        er = ExcelReader()
        result_data = er.read_excel(path, sheet_name)
        return result_data

    # 基地机场对应写入数据库
    def write_issuance(self):
        dataframe = self.read_issuance()  # dataframe[0]为要插入的一条数据
        for data in dataframe:
            print(data)
            if self.issuance.select_from_part_serial_issue_date(data) == ():  # 唯一性检验 件号+发料日期
                if data['飞机号'] is None:
                    data['飞机号'] = 'NULL'
                registration_id = self.ar.select_id_from_registration(data['飞机号'])
                warehouse_id = self.warehouse.select_id_from_warehouse_code(data['库房'])
                if registration_id != ():  # 不在已知列表则保留原有
                    data['飞机号'] = registration_id
                if warehouse_id != ():  # 不在已知列表则保留原有
                    data['库房'] = warehouse_id
                data['发料日期'] = datetime_to_string(data['发料日期'])
                self.issuance.insert_table(data)
