from datetime import date
from datetime import datetime, timedelta


def flight_schedule_to_list(schedule_str):
    """将航班计划字符串转换为包含每天是否有航班计划的列表"""
    # 定义一个包含每天是否有航班计划的列表，初始值为 0
    schedule_list = [0] * 7
    # 遍历字符串中的每个字符
    for i in range(len(schedule_str)):
        # 如果字符是数字，则将对应的列表元素设为 1
        if schedule_str[i].isdigit():
            schedule_list[int(schedule_str[i]) - 1] = 1
    return schedule_list


def get_weekday(year, month, day):
    """
    给定日期，返回该日期是一周中的哪一天（周一到周日分别用 0 到 6 表示）
    """
    weekday = date(year, month, day).weekday()
    return weekday


def datetime_to_string(dt):
    """
    规范读入数据日期，datetime.datetime(2023, 5, 16, 0, 0) -> ‘2023-05-16’
    """
    return dt.strftime('%Y-%m-%d')


def generate_dates(start_date, end_date):
    """
    生成指定日期中间所有日期
    """
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    all_dates = []

    # 遍历从开始日期到结束日期的每一天
    current_date = start_date
    while current_date <= end_date:
        # 将当前日期添加到列表中
        all_dates.append(current_date.strftime("%Y-%m-%d"))
        # 增加一天
        current_date += timedelta(days=1)

    return all_dates


def generate_replacement_date_list(start_date, end_date, replacement_info):
    """
    :param replacement_info: 二维数组,replacement_info[0]为时间，replacement_info[1]为对应时间发生拆换次数
    """
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    info_list = []

    current_date = start_date
    while current_date <= end_date:
        current_date_str = current_date.strftime("%Y-%m-%d")  # 下面判断日期是否存在时需要str
        if current_date_str in replacement_info[0]:
            index = replacement_info[0].index(current_date_str)
            replacement_num = replacement_info[1][index]
            info_list.append(replacement_num)
        else:
            info_list.append(0)
        current_date += timedelta(days=1)

    return info_list


def aggregate_data_by_month(start_date, end_date, data):
    """
    整合以日为单位成以月成单位
    """
    # 初始化月份数据字典
    monthly_data = {}
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # 遍历日期范围内的每一天
    current_date = start_date
    while current_date <= end_date:
        # 获取当前日期的年份和月份
        year = current_date.year
        month = current_date.month

        # 构建当前日期的键
        key = f"{year}-{month:02d}"

        # 如果当前键不存在，则初始化值为0
        if key not in monthly_data:
            monthly_data[key] = 0

        # 将当前日期的数据累加到对应的月份中
        idx = (current_date - start_date).days
        monthly_data[key] += data[idx]

        # 增加一天
        current_date += timedelta(days=1)

    # 将月份数据存储到列表中
    keys_list = list(monthly_data.keys())
    result = list(monthly_data.values())
    # print(monthly_data)

    return keys_list, result

