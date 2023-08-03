from Model.DA.FlightPlan import FlightPlan
from Utils.ExcelReader import ExcelReader
from Utils.tools import *


class FlightPlanDao:
    def __init__(self, db):
        self.fp = FlightPlan(db)

    def read_flightplan(self):
        path = "Files/Tables/国内航司国内航班计划（二字码） .xlsx"
        sheet_name = "2023夏秋国内计划"
        er = ExcelReader()
        result_data = er.read_excel(path, sheet_name)
        print("result_data[0]", result_data[0])
        return result_data

    def write_flightplan(self):
        dataframe = self.read_flightplan()  # dataframe[0]为要插入的一条数据
        for data in dataframe:
            schedule = flight_schedule_to_list(dataframe[0]['FD3'])
            print("dataframe[0]_schedule", schedule)

