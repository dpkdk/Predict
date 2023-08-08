import pandas as pd

from Model.DA.Base import Base
from Utils.ExcelReader import ExcelReader
from Utils.tools import *


class BaseDao:
    def __init__(self, db):
        self.base = Base(db)


    # 读基地base.txt
    def read_base(self):
        path = "Files/Tables/base.txt"
        er = ExcelReader()
        result_data = er.read_txt(path)
        return result_data

    # 基地机场对应写入数据库
    def write_base(self):
        dataframe = self.read_base()  # dataframe[0]为要插入的一条数据
        for data in dataframe:
            print(data)
            if self.base.select_from_IATA(data) == ():  # 唯一性检验 三字码
                self.base.insert_table(data)
