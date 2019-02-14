import ccxt
from DB_Local_utils_load import *
from GlobalObjects import symbols_usd

update_symbols = symbols_usd

pd.set_option('expand_frame_repr', False)
pd.set_option('max_rows', 20)

bitfinex = ccxt.bitfinex({'timeout': 1000})

timeframe = '1d'
db_name_update = 'cc_bitfinex_hd_1d'
time_sh_utc_hours_diff = 8

list_obj_tableupdate = [CallTableUpdate(symbol=i, exchange=bitfinex, timeframe=timeframe, time_sh_utc_hours_diff=time_sh_utc_hours_diff, db_name=db_name_update) for i in update_symbols]
list_obj_tableupdate_remain = run_function_list(object_list=list_obj_tableupdate)

print('=' * 100)
print([i.symbol for i in list_obj_tableupdate_remain])
