import pandas as pd

from Model.DA.AircraftRegistration import AircraftRegistration
from Model.DA.FlightHours import FlightHours
from Utils.ExcelReader import ExcelReader
from Utils.tools import *


class FlightHoursDao:
    def __init__(self, db):
        self.fh = FlightHours(db)
        self.ar = AircraftRegistration(db)

    # 读飞行小时（20220601-20230701）.xlsx
    def read_flighthours(self):
        path = "Files/Tables/飞行小时（20220601-20230701）.xlsx"
        sheet_name = "Sheet1"
        er = ExcelReader()
        result_data = er.read_excel(path, sheet_name)
        return result_data

    # 飞行小时写入数据库,关联飞机布局表（一条对应一个机号id,不属于给定机型的对应机号id就是null，因为没有对应
    def write_flighthours(self):
        dataframe = self.read_flighthours()  # dataframe[0]为要插入的一条数据
        for data in dataframe:
            print(data)
            if self.fh.select_from_registration_airbornetime(data) == ():  # 唯一性检验 机号+空中时间 2274
                registration_id = self.ar.select_id_from_registration(data['机号'])
                if registration_id == ():
                    registration_id = None
                self.fh.insert_table(data, registration_id)
