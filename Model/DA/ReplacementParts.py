import snowflake.client

from Utils.tools import *


class ReplacementParts:
    def __init__(self, db):
        self.db = db
        self.cursor = db.cursor()

    # 插入一条新数据
    def insert_table(self, data):
        id = snowflake.client.get_guid()
        sql = "INSERT INTO replacement_parts (`id`, `registration_id`, `remove_date`, `received_part_number`, `received_part_serial`, `removed_part_number`, `removed_part_serial`, `installed_part_number`, `installed_part_serial`, `component_name`, `replacement_reason`, `part_type`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        params = (id, data['机号'], data['实物拆下日期'], data['入库实物件号'], data['入库实物序号'], data['拆下件号'], data['拆下序号'], data['装上件号'], data['装上序号'], data['部件名称'], data['拆换原因'], data['件型'])
        print(sql % params)
        try:
            self.cursor.execute(sql, params)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print("Error executing SQL statement:", e)

    # 验证数据唯一性 -> 拆下序号+拆下日期
    def select_from_remove_date_removed_part_serial(self, data):
        remove_date = datetime_to_string(data['实物拆下日期'])
        removed_part_serial = data['拆下序号']
        sql = "SELECT * FROM replacement_parts WHERE remove_date=%s and removed_part_serial=%s"
        params = remove_date, removed_part_serial
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)

    # 统计各机号周转件发生拆换总次数,按次数降序--ar，rp联表
    def select_registration_count_inner(self):
        sql = "SELECT rp.registration_id,ar.registration,COUNT(*) from replacement_parts as rp INNER JOIN aircraft_registration as ar on rp.registration_id=ar.id and rp.part_type='ROTB' GROUP BY rp.registration_id ORDER BY count(*) DESC"
        params = ()
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)

    # 统计指定机号周转件发生拆换日期--
    def select_from_registration_id_and_part_type_inner(self, registration_id):
        sql = "SELECT rp.registration_id,ar.registration,rp.remove_date,COUNT(*) from replacement_parts as rp INNER JOIN aircraft_registration as ar on rp.registration_id=ar.id and rp.part_type='ROTB' and ar.id=%s GROUP BY registration_id,remove_date"
        params = registration_id
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)

    # 统计指定机号周转件发生拆换日期（精确到件号）--
    def select_id_date_part_group_from_registration_id_and_part_type_inner(self, registration_id):
        sql = "SELECT rp.registration_id,ar.registration,rp.remove_date,rp.removed_part_number,COUNT(*) from replacement_parts as rp INNER JOIN aircraft_registration as ar on rp.registration_id=ar.id and rp.part_type='ROTB' and ar.id=%s GROUP BY registration_id,remove_date,removed_part_number"
        params = registration_id
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)

    # 获取一个机型所有时间内发生拆换的所有件号及其拆换数量
    def select_all_part_number_from_registration_id(self, registration_id):
        sql = "SELECT registration_id,removed_part_number,COUNT(*) from replacement_parts WHERE registration_id=%s and part_type='ROTB' GROUP BY removed_part_number"
        params = registration_id
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)

    # 获取一个机型所有时间内发生拆换的所有件号及其拆换数量
    def select_date_from_registration_id_part_number(self, registration_id, part_number):
        sql = "SELECT registration_id,remove_date,COUNT(*) from replacement_parts WHERE registration_id=%s and removed_part_number=%s and part_type='ROTB' GROUP BY remove_date"
        params = registration_id, part_number
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)

    # 获取所有零件号
    def select_part_number_group(self):
        sql = "SELECT removed_part_number,COUNT(*) from replacement_parts WHERE part_type='ROTB' GROUP BY removed_part_number order by COUNT(*) desc"
        params = ()
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)

    # 获取指定零件号的所有拆换记录（按日期计数版
    def select_date_count_by_part_number(self, part_number):
        sql = "SELECT removed_part_number,remove_date,COUNT(*) FROM replacement_parts WHERE removed_part_number=%s and part_type='ROTB' GROUP BY removed_part_number,remove_date"
        params = part_number
        # print(sql % params)
        try:
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print("Error executing SQL statement:", e)




