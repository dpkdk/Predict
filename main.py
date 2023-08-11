import subprocess

from Model.DA.SQLConnection import SQLConnection
from Model.DAO.AircraftRegistrationDao import AircraftRegistrationDao
from Model.DAO.BaseDao import BaseDao
from Model.DAO.FlightHoursDao import FlightHoursDao
from Model.DAO.FlightPlanDao import FlightPlanDao
from Model.DAO.IssuanceDao import IssuanceDao
from Model.DAO.ReplacementPartsDao import ReplacementPartsDao
from Model.DAO.WarehouseDao import WarehouseDao

'''
1.基地，三字码，四字码查询入库，，找关系，，基地、机场
2.这三张表关系找一下
'''

def flightplan_business():
    fp_dao = FlightPlanDao(db)
    fp_dao.write_flightplan()
    # fp_dao.calculate_flight_volume()
    # fp_dao.calculate_weekly_flight_volume()
    # fp_dao.calculate_airport_flight_volume()

def aircraftregistration_business():
    ar_dao = AircraftRegistrationDao(db)
    ar_dao.write_aircraftregistration()

def base_business():
    base_dao = BaseDao(db)
    base_dao.write_base()

def flihthours_business():
    fh_dao = FlightHoursDao(db)
    fh_dao.write_flighthours()


def issuance_business():
    issuance_dao = IssuanceDao(db)
    issuance_dao.write_issuance()

def replacementparts_business():
    rp = ReplacementPartsDao(db)
    # rp.write_replacement_parts()
    # rp.get_total_replacement_count_by_all_registration()
    # rp.get_part_replacement_count_by_all_registration()  # 按机号、机号中的件号
    rp.get_replacement_count_part()  # 按件号
    # rp.get_total_replacement_count_by_all_part_number()  # 各件号所有时间发生拆换次数

def warehouse_business():
    warehouse = WarehouseDao(db)
    warehouse.write_warehouse()




if __name__ == '__main__':
    #启动 snowflake_start_server 命令
    # process = subprocess.Popen(['snowflake_start_server', '--worker=1'])

    # **********************************************************************************************************************

    db = SQLConnection().sql_connection()

    # flightplan_business()
    # aircraftregistration_business()
    # base_business()
    # flihthours_business()
    # issuance_business()
    replacementparts_business()
    # warehouse_business()








    # **********************************************************************************************************************

    # 在程序退出时，停止 snowflake_start_server 进程
    # process.kill()
