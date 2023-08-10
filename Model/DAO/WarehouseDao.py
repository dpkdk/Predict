import pandas as pd

from Model.DA.Base import Base
from Model.DA.Warehouse import Warehouse
from Utils.ExcelReader import ExcelReader
from Utils.tools import *


class WarehouseDao:
    def __init__(self, db):
        self.warehouse = Warehouse(db)
        self.base = Base(db)


    # 读库房代码对应名称.xls
    def read_warehouse(self):
        path = "Files/Tables/库房代码对应名称.xls"
        sheet_name = "SQL Results"
        er = ExcelReader()
        result_data = er.read_excel(path, sheet_name)
        return result_data

    # 库房代码对应名称对应写入数据库
    def write_warehouse(self):
        dataframe = self.read_warehouse()  # dataframe[0]为要插入的一条数据
        for data in dataframe:
            station_id = self.base.select_id_from_IATA(data['STATION'])
            if station_id != ():  # 站点在20内，替换为id;若站点不在已知20里，保留原IATA
                data['STATION'] = station_id
            print(data)
            if self.warehouse.select_from_warehouse_code(data) == ():  # 唯一性检验 唯一仓库妈
                self.warehouse.insert_table(data)
