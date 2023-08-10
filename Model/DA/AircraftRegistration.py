import snowflake.client

from Utils.tools import *


class AircraftRegistration:
    def __init__(self, db):
        self.db = db
        self.cursor = db.cursor()

    # 插入一条新数据
    def insert_table(self, data, base_id):
        id = snowflake.client.get_guid()
        sql = "INSERT INTO aircraft_registration (`id`, `registration`, `aircraft_type`, `governing_unit`, `base_id`) VALUES (%s, %s, %s, %s, %s)"
        params = (id, data['注册号'], data['机型'], data['执管单位'], base_id)
        print(sql % params)
        try:
            self.cursor.execute(sql, params)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print("Error executing SQL statement:", e)

    # 验证数据唯一性 -> 飞机注册号
    def select_from_registration(self, data):
        registration = data['注册号']
        sql = "SELECT * FROM aircraft_registration WHERE registration=%s"
        params = registration
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)

    # 根据机号查询该数据项id
    def select_id_from_registration(self, registration):
        sql = "SELECT id FROM aircraft_registration WHERE registration=%s"
        params = registration
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)

    # 根据id查询该机号
    def select_registration_from_id(self, registration_id):
        sql = "SELECT registration FROM aircraft_registration WHERE id=%s"
        params = registration_id
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)




