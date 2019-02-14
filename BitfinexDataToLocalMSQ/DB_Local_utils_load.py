from sqlalchemy import create_engine
import pandas as pd
import datetime
import time


def local_db_exists(db_name):
    '''
    检测本地mysql数据库是否含有数据库 db_name
    :param db_name: str
    :return: bool
    '''
    check_engine = create_engine('mysql://root:123456@localhost')
    try:
        check_engine.execute(f"use {db_name}")
        return True
    except:
        return False


def local_fresh_db_con(db_name_title):
    '''
    在本地mysql数据库新建一个数据库，数据库名为 db_name_title 加上当天日期字符串
    :param db_name_title: str
    :return: [一个指向新建好的数据库的 sqlalchemy 链接，新数据名字字符串]
    '''
    db_name = db_name_title + '_' + str(datetime.datetime.now())[:10].replace('-', '_')
    engine = create_engine('mysql://root:123456@localhost')
    if local_db_exists(db_name=db_name):
        engine.execute(f"drop database {db_name}")
        engine.execute(f"create database {db_name}")
    else:
        engine.execute(f"create database {db_name}")
    engine.execute(f"use {db_name}")
    return [engine, db_name]


def cal_time_delta(timeframe):
    '''
    将字符串时间间隔转换成pandas.Timedelta对象
    '''
    frame = timeframe[-1]
    unit = int(timeframe[:-1])
    if frame == 'm':
        return pd.Timedelta(minutes=unit)
    elif frame == 'h':
        return pd.Timedelta(hours=unit)
    elif frame == 'd':
        return pd.Timedelta(days=unit)
    else:
        return None


def ccxt_fetch_start_k_data(exchange, symbol, timeframe, limit, time_sh_utc_hours_diff):
    '''
    获取exchange交易所内symbol交易对最初的limit根timeframe K线数据
    :param exchange: ccxt的exchange对象
    :param symbol: 字符串，例如"BTC/USD"
    :param timeframe: 字符串，例如"5m"、"1d"
    :param limit: 整形，例如1000
    :param time_sh_utc_hours_diff: int，当地时间与utc时间的小时差，例如在上海就选择8
    :return: pandas.DataFrame，其中的时间（time）为当地时间
    '''
    data = exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=limit, since=0)
    result = pd.DataFrame(data)
    result.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
    if timeframe[-1] != 'd':
        result['time'] = pd.to_datetime(result['time'], unit='ms') + pd.Timedelta(hours=time_sh_utc_hours_diff)
    result['time'] = pd.to_datetime(result['time'], unit='ms')
    result.drop_duplicates(subset=['time'], inplace=True)
    result.sort_values(by=['time'], ascending=True, inplace=True)
    result = result.iloc[:-1, :]
    return result


def ccxt_fetch_continue_k_data(exchange, symbol, timeframe, start_time_str, time_sh_utc_hours_diff, limit):
    '''
    获取exchange交易所内symbol交易对从当地时间start_time_str开始的limit根timeframe K线数据
    :param exchange: ccxt的exchange对象
    :param symbol: 字符串，例如"BTC/USD"
    :param timeframe: 字符串，例如"5m"、"1d"
    :param start_time_str: 字符串，例如"2019-01-01"
    :param time_sh_utc_hours_diff: int，当地时间与utc时间的小时差，例如在上海就选择8
    :param limit: 整形，例如1000
    :return: pandas.DataFrame，其中的时间（time）为当地时间
    '''
    local_timestamp = pd.to_datetime(start_time_str).timestamp()
    utc_timestamp = local_timestamp - time_sh_utc_hours_diff * 60 * 60
    data = exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=limit, since=utc_timestamp * 1000)
    result = pd.DataFrame(data)
    result.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
    if timeframe[-1] != 'd':
        result['time'] = pd.to_datetime(result['time'], unit='ms') + pd.Timedelta(hours=time_sh_utc_hours_diff)
    result['time'] = pd.to_datetime(result['time'], unit='ms')
    result.drop_duplicates(subset=['time'], inplace=True)
    result.sort_values(by=['time'], ascending=True, inplace=True)
    result = result.iloc[:-1, :]
    return result


def local_table_fresh(exchange, symbol, timeframe, db_con, time_sh_utc_hours_diff):
    '''
    在db_con指向的本地mysql数据库中下载exchange交易所symbol交易对的timeframe K线数据
    :param exchange: ccxt的exchange对象
    :param symbol: 字符串，例如"BTC/USD"
    :param timeframe: 字符串，例如"5m"、"1d"
    :param db_con: sqlalchemy.create_engine()创建的链接
    :param time_sh_utc_hours_diff: int，当地时间与utc时间的小时差，例如在上海就选择8
    :return: 此函数会将symbol交易对在当前时间前两天之前的所有历史数据全部下载到本地的mysql数据库，直到出现网络异常或者下载完全之后停止。
    '''
    name_table = ('_'.join(symbol.split('/')) + '_' + timeframe).lower()
    db_date = str(datetime.datetime.now() - pd.Timedelta(days=2))[:10]
    df_first = run_function_till_success(function=lambda: ccxt_fetch_start_k_data(exchange=exchange, symbol=symbol, timeframe=timeframe, limit=1000, time_sh_utc_hours_diff=time_sh_utc_hours_diff), tryTimes=5, sleepTimes=1)[0]
    df_first.to_sql(name=name_table, if_exists='replace', con=db_con, index=False)
    stop = 0
    while True:
        if stop >= 2:
            break
        try:
            query = f'select * from {name_table} order by time desc limit 5'
            record_last = pd.read_sql(sql=query, con=db_con).sort_values(by=['time'], ascending=True)
            time_last = str(record_last['time'].iloc[-1])
            print(f'loading {symbol} {timeframe} {time_last}')
            if time_last[:10] >= db_date:
                break
            time_delta = cal_time_delta(timeframe=timeframe)
            data_new = run_function_till_success(function=lambda: ccxt_fetch_continue_k_data(exchange=exchange, symbol=symbol, timeframe=timeframe, start_time_str=str(pd.to_datetime(time_last) + time_delta), time_sh_utc_hours_diff=time_sh_utc_hours_diff, limit=1000), tryTimes=5, sleepTimes=1)[0]
            if data_new.empty:
                break
            else:
                data_new.to_sql(name=name_table, con=db_con, if_exists='append', index=False)
        except:
            stop += 1
        else:
            time.sleep(3)


def local_table_update(exchange, symbol, timeframe, db_name, time_sh_utc_hours_diff):
    '''
    更新db_name数据库中exchange交易所symbol交易对的timeframe K线数据
    :param exchange: ccxt的exchange对象
    :param symbol: 字符串，例如"BTC/USD"
    :param timeframe: 字符串，例如"5m"、"1d"
    :param db_name: str，数据库名字
    :param time_sh_utc_hours_diff: int，当地时间与utc时间的小时差，例如在上海就选择8
    :return: 此函数会将symbol交易对在当前时间前两天之前的所有历史数据进行更新，即下载目前未下载到的数据并且保存到本地的mysql数据库，直到出现网络异常或者下载完全之后停止。
    '''
    name_table = ('_'.join(symbol.split('/')) + '_' + timeframe).lower()
    engine = create_engine('mysql://root:123456@localhost')
    engine.execute(f"use {db_name}")
    db_date = str(datetime.datetime.now() - pd.Timedelta(days=2))[:10]
    stop = 0
    while True:
        if stop >= 2:
            break
        try:
            query = f'select * from {name_table} order by time desc limit 5'
            record_last = pd.read_sql(sql=query, con=engine).sort_values(by=['time'], ascending=True)
            time_last = str(record_last['time'].iloc[-1])
            print(f'loading {symbol} {timeframe} {time_last}')
            if time_last[:10] >= db_date:
                break
            time_delta = cal_time_delta(timeframe=timeframe)
            data_new = run_function_till_success(function=lambda: ccxt_fetch_continue_k_data(exchange=exchange, symbol=symbol, timeframe=timeframe, start_time_str=str(pd.to_datetime(time_last) + time_delta), time_sh_utc_hours_diff=time_sh_utc_hours_diff, limit=1000), tryTimes=5, sleepTimes=1)[0]
            if data_new.empty:
                break
            else:
                data_new.to_sql(name=name_table, con=engine, if_exists='append', index=False)
        except:
            stop += 1
        else:
            time.sleep(3)


def run_function_till_success(function, tryTimes, sleepTimes):
    retry = 0
    while True:
        if retry > tryTimes:
            return False
        try:
            result = function()
            return [result, retry]
        except:
            retry += 1
            time.sleep(sleepTimes)


def revert_dict(dict_original):
    assert len(set(dict_original.values())) == len(dict_original), 'original dict values must be different'
    return {v: k for k, v in dict_original.items()}


def list_dict_values(dict_original):
    return list(dict_original.values())


def list_dict_keys(dict_original):
    return list(dict_original.keys())


def dict_extract_by_keys(dict_the, list_the):
    return {k: dict_the.get(k) for k in list_the if k in list_dict_keys(dict_the)}


def run_function_list(object_list):
    wait_obj_list = object_list.copy()
    stop = 0
    while wait_obj_list and stop < 2:
        prenum = len(wait_obj_list)
        do_list = wait_obj_list.copy()
        wait_obj_list = []
        for obj in do_list:
            try:
                obj.call_run_function_list()
                print('completed', obj.symbol, sep='\t')
            except:
                wait_obj_list.append(obj)
                print(obj.symbol, '-' * 100)
        if prenum == len(wait_obj_list):
            stop += 1
        elif prenum > len(wait_obj_list):
            stop = 0
        print('this loop remain: ', '-' * 100)
        print([i.symbol for i in wait_obj_list])
    print('finally remain: ', '-' * 100)
    print([i.symbol for i in wait_obj_list])
    return wait_obj_list


class CallTableFresh:
    def __init__(self, symbol, exchange, timeframe, time_sh_utc_hours_diff, db_con):
        self.symbol = symbol
        self.exchange = exchange
        self.timeframe = timeframe
        self.time_sh_utc_hours_diff = time_sh_utc_hours_diff
        self.db_con = db_con

    def call_run_function_list(self):
        local_table_fresh(exchange=self.exchange, symbol=self.symbol, timeframe=self.timeframe, db_con=self.db_con, time_sh_utc_hours_diff=self.time_sh_utc_hours_diff)


class CallTableUpdate:
    def __init__(self, symbol, exchange, timeframe, db_name, time_sh_utc_hours_diff):
        self.symbol = symbol
        self.exchange = exchange
        self.timeframe = timeframe
        self.db_name = db_name
        self.time_sh_utc_hours_diff = time_sh_utc_hours_diff

    def call_run_function_list(self):
        local_table_update(exchange=self.exchange, symbol=self.symbol, timeframe=self.timeframe, db_name=self.db_name, time_sh_utc_hours_diff=self.time_sh_utc_hours_diff)
