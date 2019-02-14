import ccxt
import pandas as pd
import time
from GlobalObjects import print_list

pd.set_option('expand_frame_repr', False)
pd.set_option('max_rows', 20)

bitfinex = ccxt.bitfinex({'timeout': 1000})

bitfinex.load_markets()
symbols = bitfinex.symbols
symbols_usd=[i for i in symbols if i[-3:]=='USD']

print_list(symbols)
print('\n'+'-'*100+'\n')
print_list(symbols_usd)
