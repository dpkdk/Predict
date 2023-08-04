import snowflake.client

from Utils.tools import *


class FlightPlan:
    def __init__(self, db):
        self.db = db
        self.cursor = db.cursor()

    # 插入新数据（带周一周二。。
    def insert_table(self, data):
        schedule = flight_schedule_to_list(data['FD3'])
        id = snowflake.client.get_guid()
        sql = "INSERT INTO flightplan (`id`, `FD1`, `FD2`, `FD3`, `FD4`, `FD5`, `FD6`, `FD7`, `FD8`, `FD9`, `FD10`, `FD11`, `FD12`, `FD13`, `FD19`, `FD21`, `FD23`, `FD24`, `FD26`, `Z1`, `Z2`, `Z3`, `Z4`, `Z5`, `Z6`, `Z7`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        params = (id, data['FD1'], data['FD2'], data['FD3'], data['FD4'], data['FD5'], data['FD6'], data['FD7'], data['FD8'], data['FD9'], data['FD10'], data['FD11'], data['FD12'], data['FD13'], data['FD19'], data['FD21'], data['FD23'], data['FD24'], data['FD26'], schedule[0], schedule[1], schedule[2], schedule[3], schedule[4], schedule[5], schedule[6])
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print("Error executing SQL statement:", e)

    # 查询*只有*特定周几有飞行计划的航班
    def select_FD1245678910111213_from_onlyZ(self, weeklist):
        sql = "SELECT FD1,FD2,FD4,FD5,FD6,FD7,FD8,FD9,FD10,FD11,FD12,FD13 FROM flightplan WHERE Z1=%s and Z2=%s and Z3=%s and Z4=%s and Z5=%s and Z6=%s and Z7=%s"
        params = (weeklist[0], weeklist[1], weeklist[2], weeklist[3], weeklist[4], weeklist[5], weeklist[6])
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)
    # 查询所有周几有飞行计划的航班
    def select_FD1245678910111213_from_Z(self, z):
        sql = "SELECT FD1,FD2,FD4,FD5,FD6,FD7,FD8,FD9,FD10,FD11,FD12,FD13 FROM flightplan WHERE {}=1".format(z)
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)


    # 按FD4机场计数，即一次起飞机场
    def groupby_FD4(self, z):
        sql = "SELECT FD4, COUNT(*) FROM flightplan WHERE {} = '1' GROUP BY FD4".format(z)
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)

    # 按FD7机场计数，即一次降落机场
    def groupby_FD7(self, z):
        sql = "SELECT FD7, COUNT(*) FROM flightplan WHERE {} = '1' GROUP BY FD7".format(z)
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)




