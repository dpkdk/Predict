import pandas as pd

from Model.DA.FlightPlan import FlightPlan
from Utils.ExcelReader import ExcelReader
from Utils.tools import *


class FlightPlanDao:
    def __init__(self, db):
        self.fp = FlightPlan(db)
        self.weekday_map = {0: 'Z1', 1: 'Z2', 2: 'Z3', 3: 'Z4', 4: 'Z5', 5: 'Z6', 6: 'Z7'}


    # 读国内航司国内航班计划（二字码） .xlsx
    def read_flightplan(self):
        path = "Files/Tables/国内航司国内航班计划（二字码） .xlsx"
        sheet_name = "2023夏秋国内计划"
        er = ExcelReader()
        result_data = er.read_excel(path, sheet_name)
        return result_data

    # 国内航司国内航班计划（二字码） .xlsx写入数据库，加Z1，Z2.。字段
    def write_flightplan(self):
        dataframe = self.read_flightplan()  # dataframe[0]为要插入的一条数据
        for data in dataframe:
            print(data)
            self.fp.insert_table(data)

    # 查询特定周几航班计划
    def select_weekplan(self):
        weeklist = [1, 0, 1, 0, 0, 0, 0]  # 等效于..1...1
        res = self.fp.select_FD1245678910111213_from_Z(weeklist)

    # 查询周几有飞行计划的所有航班
    def select_zplan(self, z):
        result = self.fp.select_FD1245678910111213_from_Z(z)
        return result

    # 计算指定日期航班量
    def calculate_flight_volume(self):
        weekday = get_weekday(2023, 8, 4)
        z = self.weekday_map.get(weekday, None)
        result = self.select_zplan(z)
        print(f"这天是{z},计划航班量有{len(result)}")

    # 计算一周内每天的航班量
    def calculate_weekly_flight_volume(self):
        weeklist = list(self.weekday_map.values())
        weekly_flight_volume = []
        print(weeklist)
        for z in weeklist:
            zvolume = len(self.select_zplan(z))
            weekly_flight_volume.append(zvolume)
            print(f"{z}计划航班量{zvolume}")
        print(weekly_flight_volume)

    # 按机场统计航班量
    def calculate_airport_flight_volume(self):
        weekday = get_weekday(2023, 8, 4)
        z = self.weekday_map.get(weekday, None)
        result1_1 = self.fp.groupby_FD4(z)
        for res in result1_1:
            print(f"该日从{res[0]}发出航班{res[1]}架次")
        result1_2 = self.fp.groupby_FD7(z)
        for res in result1_2:
            print(f"该日从{res[0]}降落航班{res[1]}架次")
