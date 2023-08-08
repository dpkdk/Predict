import snowflake.client

from Utils.tools import *


class Base:
    def __init__(self, db):
        self.db = db
        self.cursor = db.cursor()

    # 插入一条新数据
    def insert_table(self, data):
        id = snowflake.client.get_guid()
        sql = "INSERT INTO base (`id`, `city`, `airport`, `IATA`, `ICAO`) VALUES (%s, %s, %s, %s, %s)"
        params = (id, data['city'], data['airport'], data['IATA'], data['ICAO'])
        print(sql % params)
        try:
            self.cursor.execute(sql, params)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print("Error executing SQL statement:", e)

    # 验证数据唯一性 -> 三字码
    def select_from_IATA(self, data):
        IATA = data['IATA']
        sql = "SELECT * FROM base WHERE IATA=%s"
        params = IATA
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




