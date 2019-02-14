import pandas as pd
from sqlalchemy import create_engine, inspect

pd.set_option('expand_frame_repr', False)
pd.set_option('max_rows', 20)


def DB_get_one_field_all(db_name='cc_bitfinex_hd_1d', field='close', indextostr=True, indextostrdigits=10):
    engine = create_engine(f'mysql://root:123456@localhost/{db_name}')
    inspector = inspect(engine)
    list_table = inspector.get_table_names()
    list_table.sort()
    list_coin = [i.split('_')[0] for i in list_table]
    result_DF = pd.DataFrame()
    for table, coin in zip(list_table, list_coin):
        data = pd.read_sql_table(table_name=table, con=engine)
        data.index = data['time']
        result = data[field]
        result.name = coin
        result_DF = pd.concat([result_DF, result], axis=1, sort=False)
    if indextostr:
        result_DF.index = pd.Series(result_DF.index).apply(lambda x: str(x)[:indextostrdigits])
    return result_DF


def DB_get_one_field_forList(db_name, field, coin_list, indextostr=True, indextostrdigits=10):
    engine = create_engine(f'mysql://root:123456@localhost/{db_name}')
    inspector = inspect(engine)
    list_table = inspector.get_table_names()
    list_table.sort()
    list_coin = [i.split('_')[0] for i in list_table]
    result_DF = pd.DataFrame()
    for table, coin in zip(list_table, list_coin):
        if coin in coin_list:
            data = pd.read_sql_table(table_name=table, con=engine)
            data.index = data['time']
            result = data[field]
            result.name = coin
            result_DF = pd.concat([result_DF, result], axis=1, sort=False)
    if indextostr:
        result_DF.index = pd.Series(result_DF.index).apply(lambda x: str(x)[:indextostrdigits])
    return result_DF


def DB_get_usd_AMT_all(db_name='cc_bitfinex_hd_1d', indextostr=True, indextostrdigits=10):
    engine = create_engine(f'mysql://root:123456@localhost/{db_name}')
    inspector = inspect(engine)
    list_table = inspector.get_table_names()
    list_table.sort()
    list_coin = [i.split('_')[0] for i in list_table]
    result_DF = pd.DataFrame()
    for table, coin in zip(list_table, list_coin):
        data = pd.read_sql_table(table_name=table, con=engine)
        data['avgp'] = data[['open', 'high', 'low', 'close']].mean(axis=1)
        data['amt'] = data['avgp'] * data['volume']
        data.index = data['time']
        result = data['amt']
        result.name = coin
        result_DF = pd.concat([result_DF, result], axis=1, sort=False)
    if indextostr:
        result_DF.index = pd.Series(result_DF.index).apply(lambda x: str(x)[:indextostrdigits])
    return result_DF


def DB_get_usd_AMT_forList(db_name, coin_list, indextostr=True, indextostrdigits=10):
    engine = create_engine(f'mysql://root:123456@localhost/{db_name}')
    inspector = inspect(engine)
    list_table = inspector.get_table_names()
    list_table.sort()
    list_coin = [i.split('_')[0] for i in list_table]
    result_DF = pd.DataFrame()
    for table, coin in zip(list_table, list_coin):
        if coin in coin_list:
            data = pd.read_sql_table(table_name=table, con=engine)
            data['avgp'] = data[['open', 'high', 'low', 'close']].mean(axis=1)
            data['amt'] = data['avgp'] * data['volume']
            data.index = data['time']
            result = data['amt']
            result.name = coin
            result_DF = pd.concat([result_DF, result], axis=1, sort=False)
    if indextostr:
        result_DF.index = pd.Series(result_DF.index).apply(lambda x: str(x)[:indextostrdigits])
    return result_DF
