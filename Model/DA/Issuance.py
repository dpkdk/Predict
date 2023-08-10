import snowflake.client

from Utils.tools import *


class Issuance:
    def __init__(self, db):
        self.db = db
        self.cursor = db.cursor()

    # 插入一条新数据
    def insert_table(self, data):
        id = snowflake.client.get_guid()
        sql = "INSERT INTO issuance (`id`, `part_number`, `part_serial`, `quantity`, `equipment_status`, `issue_purpose`, `issue_date`, `registration_id`, `warehouse_id`, `time_life`, `part_name`, `equipment_aircraft_model`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        params = (id, data['件号'], data['序号'], data['数量'], data['器材状态'], data['发料目的'], data['发料日期'], data['飞机号'], data['库房'], data['时寿'], data['名称'], data['器材所属机型'])
        print(sql % params)
        try:
            self.cursor.execute(sql, params)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print("Error executing SQL statement:", e)

    # 验证数据唯一性 -> 序号+时间
    def select_from_part_serial_issue_date(self, data):
        part_serial = data['序号']
        issue_date = data['发料日期']
        sql = "SELECT * FROM issuance WHERE part_serial=%s and issue_date=%s"
        params = part_serial, issue_date
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)

    # 已知city得到数据项id
    def select_id_from_city(self, city):
        sql = "SELECT id FROM base WHERE city=%s"
        params = city
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)




