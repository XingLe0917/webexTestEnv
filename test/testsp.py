import datetime

if __name__ == "__main__":
    timeformat = "%Y-%m-%d %H:%M:%S"
    start_time = "2020-05-06 04:20:00"
    end_time="2020-05-06 04:50:00"
    l_start_time = datetime.datetime.strptime(start_time, timeformat)
    l_end_time = datetime.datetime.strptime(end_time, timeformat)
    print(l_start_time)
    print(l_end_time)