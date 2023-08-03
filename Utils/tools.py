def flight_schedule_to_list(schedule_str):
    """将航班计划字符串转换为包含每天是否有航班计划的列表"""
    # 定义一个包含每天是否有航班计划的列表，初始值为 0
    schedule_list = [0] * 7
    # 遍历字符串中的每个字符
    for i in range(len(schedule_str)):
        # 如果字符是数字，则将对应的列表元素设为 1
        if schedule_str[i].isdigit():
            schedule_list[int(schedule_str[i])-1] = 1
    return schedule_list