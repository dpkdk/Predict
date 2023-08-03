import subprocess
from Model.DA.SQLConnection import SQLConnection
from Model.DAO.FlightPlanDao import FlightPlanDao






if __name__ == '__main__':
    # 启动 snowflake_start_server 命令
    process = subprocess.Popen(['snowflake_start_server', '--worker=1'])


    db = SQLConnection().sql_connection()
    fp_dao = FlightPlanDao(db)
    fp_dao.write_flightplan()

    # 在程序退出时，停止 snowflake_start_server 进程
    process.kill()


