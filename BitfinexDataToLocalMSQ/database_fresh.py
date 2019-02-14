import ccxt
from DB_Local_utils_load import *
from GlobalObjects import symbols_usd, coins_picked

fresh_symbols = [i for i in symbols_usd if i.split('/')[0].lower() in coins_picked[:10]]

pd.set_option('expand_frame_repr', False)
pd.set_option('max_rows', 20)

bitfinex = ccxt.bitfinex({'timeout': 1000})

db_title = 'cc_bitfinex_top10_hd'
timeframe = '1h'
db_name = '_'.join([db_title, timeframe])
db_con, db_name_new = local_fresh_db_con(db_name_title=db_name)
time_sh_utc_hours_diff = 8

list_obj_tablefresh = [CallTableFresh(symbol=i, exchange=bitfinex, timeframe=timeframe, time_sh_utc_hours_diff=time_sh_utc_hours_diff, db_con=db_con) for i in fresh_symbols]

list_obj_tablefresh_remain = run_function_list(object_list=list_obj_tablefresh)

print('=' * 100)
print([i.symbol for i in list_obj_tablefresh_remain])
