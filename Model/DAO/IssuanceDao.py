import pandas as pd

from Model.DA.Issuance import Issuance
from Utils.ExcelReader import ExcelReader
from Utils.tools import *


class IssuanceDao:
    def __init__(self, db):
        self.issuance = Issuance(db)


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
            # if self.base.select_from_IATA(data) == ():  # 唯一性检验 三字码
            #     self.base.insert_table(data)
