# symbols_usd, coins_picked的生成见print_all_symbols.py和print_coins_picked_by_amt.py

symbols_usd = ['ABYSS/USD', 'AGI/USD', 'AID/USD', 'AION/USD', 'ANT/USD', 'ATMI/USD', 'AUC/USD', 'AVT/USD', 'BAT/USD', 'BBN/USD', 'BCHABC/USD', 'BCHSV/USD', 'BCI/USD', 'BFT/USD', 'BNT/USD', 'BOX/USD', 'BTC/USD', 'BTG/USD', 'CBT/USD', 'CFI/USD', 'CND/USD', 'CNN/USD', 'CSX/USD', 'CTXC/USD', 'DADI/USD',               'DAI/USD', 'DASH/USD', 'DATA/USD', 'DGB/USD', 'DGX/USD', 'DRN/USD', 'DTA/USD', 'DTH/USD', 'EDO/USD', 'ELF/USD', 'ENJ/USD', 'EOS/USD', 'ESS/USD', 'ETC/USD', 'ETH/USD', 'ETP/USD', 'EUT/USD', 'FSN/USD', 'FUN/USD', 'GNT/USD', 'GOT/USD', 'GSD/USD', 'INT/USD', 'IOST/USD', 'IOTA/USD','IQ/USD', 'KNC/USD', 'LRC/USD', 'LTC/USD', 'LYM/USD', 'MAN/USD', 'MANA/USD', 'MGO/USD', 'MITH/USD', 'MKR/USD', 'MLN/USD', 'MTN/USD', 'NCASH/USD', 'NEO/USD', 'NIO/USD', 'ODE/USD', 'OMG/USD', 'OMN/USD', 'ONL/USD', 'PAI/USD', 'PAX/USD', 'PNK/USD', 'POA/USD', 'POLY/USD', 'QASH/USD',               'QTUM/USD', 'RBT/USD', 'RCN/USD', 'RDN/USD', 'REP/USD', 'REQ/USD', 'RLC/USD', 'RRT/USD', 'RTE/USD', 'SAN/USD', 'SEER/USD', 'SEN/USD', 'SNGLS/USD', 'SNT/USD', 'SPANK/USD', 'STORJ/USD', 'TKN/USD', 'TNB/USD', 'TRX/USD', 'TSD/USD', 'UDC/USD', 'UST/USD', 'UTK/USD', 'UTNP/USD', 'VEE/USD','VET/USD', 'VLD/USD', 'WAX/USD', 'WLO/USD', 'WPR/USD', 'WTC/USD', 'XLM/USD', 'XMR/USD', 'XRA/USD', 'XRP/USD', 'XTZ/USD', 'XVG/USD', 'YGG/USD', 'YOYOW/USD', 'ZCN/USD', 'ZEC/USD', 'ZIL/USD', 'ZRX/USD']

coins_picked = ['btc','eth','eos','xrp','ltc','neo','etc','iota','bchabc','dash','bchsv','xmr','zec','omg','mgo','trx','btg','zrx','etp','xlm']

def print_list(inputList):
    print("'", end='')
    for item in inputList[:-1]:
        print(item, "','", sep='', end='')
    print(inputList[-1], "'", sep='')
