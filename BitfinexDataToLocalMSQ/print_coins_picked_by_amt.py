import pandas as pd
from DB_Local_utils_Get import *
from GlobalObjects import print_list

pd.set_option('expand_frame_repr', False)
pd.set_option('max_rows', 20)

db_name = 'cc_bitfinex_hd_1d'
DF_amt = DB_get_usd_AMT_all(db_name=db_name, indextostr=True, indextostrdigits=10)
DF_amt = DF_amt[DF_amt.index >= '2018-12-01']

amt_avg = DF_amt.mean(skipna=True).sort_values(ascending=False)
coins_picked = amt_avg[amt_avg >= 1 * 1e5].index.tolist()

print_list(coins_picked)
print()
print(len(coins_picked))
