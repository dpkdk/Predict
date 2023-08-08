import snowflake.client

from Utils.tools import *


class FlightHours:
    def __init__(self, db):
        self.db = db
        self.cursor = db.cursor()

    # 插入一条新数据
    def insert_table(self, data, registration_id):
        print(registration_id)
        id = snowflake.client.get_guid()
        sql = "INSERT INTO flight_hours (`id`, `registration`, `aircraft_type`, `operating_time`, `airborne_time`, `ground_time`, `operating_landings_num`, `normal_landings_num`, `registration_id`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        params = (id, data['机号'], data['机型'], data['营运时间'], data['空中时间'], data['空地时间'], data['营运起落'], data['正常起落'], registration_id)
        print(sql % params)
        try:
            self.cursor.execute(sql, params)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print("Error executing SQL statement:", e)

    # 验证数据唯一性 -> 机号 + 空中时间
    def select_from_registration_airbornetime(self, data):
        registration = data['机号']
        airborne_time = data['空中时间']
        sql = "SELECT * FROM flight_hours WHERE registration=%s and ABS(airborne_time- %s) < 1e-3"  # float判断无法直接=
        params = (registration, airborne_time)
        print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)




