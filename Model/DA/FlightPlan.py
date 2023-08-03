import snowflake.client


class FlightPlan:
    def __init__(self, db):
        self.db = db
        self.cursor = db.cursor()
    def insert_table(self):
        id = snowflake.client.get_guid()
        print("id:", id)
        # sql = "INSERT INTO material_data (`id`, `material_name`, `base_id`, `aircraft_id`, `create_time`, `alter_time`, `is_deleted`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        # params = (id)
        # # print(sql % params)
        # try:
        #     # 执行sql语句
        #     self.cursor.execute(sql, params)
        #     # 提交到数据库执行
        #     self.db.commit()
        #     # print("submitted!")
        # except:
        #     # 如果发生错误则回滚
        #     self.db.rollback()
        #     print("出错啦！！！_insert_material_data")

