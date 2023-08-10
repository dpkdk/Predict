import snowflake.client

from Utils.tools import *


class Warehouse:
    def __init__(self, db):
        self.db = db
        self.cursor = db.cursor()

    # 插入一条新数据
    def insert_table(self, data):
        id = snowflake.client.get_guid()
        sql = "INSERT INTO warehouse (`id`, `warehouse_code`, `comment_text`, `station_id`, `station_facility`) VALUES (%s, %s, %s, %s, %s)"
        params = (id, data['WAREHOUSE'], data['COMMENT_TEXT'], data['STATION'], data['STATION_FACILITY'])
        print(sql % params)
        try:
            self.cursor.execute(sql, params)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print("Error executing SQL statement:", e)

    # 验证数据唯一性 -> 仓库代码
    def select_from_warehouse_code(self, data):
        warehouse_code = data['WAREHOUSE']
        sql = "SELECT * FROM warehouse WHERE warehouse_code=%s"
        params = warehouse_code
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)



    # 已知库房代码得到数据项id
    def select_id_from_warehouse_code(self, warehouse_code):
        sql = "SELECT id FROM warehouse WHERE warehouse_code=%s"
        params = warehouse_code
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)




