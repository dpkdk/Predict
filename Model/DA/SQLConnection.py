import pymysql


class SQLConnection:
    def __init__(self):
        host = '101.42.254.217'
        user = 'root'
        password = '000304Lhr'
        database = 'PREDICT'
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def sql_connection(self):
        db = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        return db



