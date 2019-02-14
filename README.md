# Bitfinex-Data-To-Local-MySQL
下载以及更新bitfinex所有交易对历史K线数据到本地mysql数据库的方案

目的：为bitfinex数字货币策略回测提供数据支持，本方案充分利用了MySQL储存数据占用空间小、存取速度快的优点，以及pandas库与sqlalchemy库对MySQL数据库的完美支持。

准备工作：
1，安装MySQL数据库到本地，并且打开MySQL服务，示例代码中我的MySQL数据库账号信息为（账户名：root，密码：123456，主机地址：localhost），在代码中将其内容依次替换成你自己的账号信息即可。
2，安装2019年1月份最新版本的pandas、ccxt、sqlalchemy库，如果已经有这几个库那就更新到2019年1月份的最新版本。
3，科学上网环境，如果是本机下载数据，请调成全局代理模式。

如果您没有耐心，可以直接跳过以下两条虚线内的介绍说明内容：
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
sqlalchemy库中建立本地MySQL数据库连接的方法，可参考sqlalchemy的文档或者：
https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql_table.html#pandas.read_sql_table
等pandas的文档，建立本地MySQL数据库连接的示例脚本如下：
from sqlalchemy import create_engine
# 例如账号信息为：（账户名：root，密码：123456，主机地址：由于登陆本地的mysql，所以填localhost）
# 实际情况中可根据自己的账户密码填写。
engine = create_engine('mysql://root:123456@localhost')

GlobalObjects.py中的symbols_usd是bitfinex交易所所有以USD为计价单位的交易对列表，coins_picked是这些交易对在2018年12月之后至今的一段时间内按照USD交易额由大到小排名的前20位币种列表。
symbols_usd, coins_picked的生成见print_all_symbols.py和print_coins_picked_by_amt.py

数据库命名规范：无强制命名规范
示例中使用cc_bitfinex_hd_timeframe的格式，例如bitfinex中选定的交易对的5分钟K线数据会存入数据库cc_bitfinex_hd_5m中，其中不同的交易对历史数据分别存入不同表中。表的命名规范如下：
数据表命名规范：格式为base_quote_timeframe
例如对于5分钟的BTC/USD交易对k线数据，将其存入数据表btc_usd_5m中。

方案设计思路：
database_fresh.py会创建一个新的数据库，后缀为K线周期，然后将需要下载数据的交易对的历史数据下载到这个数据库中，这些交易对名称存在列表变量fresh_symbols中，每个交易对的K线数据在一张表中，表的命名规范见上一条，为了不让新创建的数据库覆盖掉之前的数据库，新库库名后面会加上当日的字符串（库创建好之后库名可以修改）。
database_update.py会在本地MySQL的指定数据库db_name_update中将需要更新的交易对数据进行更新，其中需要更新的交易对存放在列表变量update_symbols中，对于其中的每一个交易对，程序会找到最后一条K线时间点，然后从这里接着下载接下来的数据并将其接入这张表中。
例如最后一张图中我在2019-01-16使用database_fresh.py创建了新的日K线数据库，然后在2019-02-14日使用database_update.py将这个库进行了更新。

使用database_fresh.py下载数据时会将所有的交易对数据进行下载，对于没有下载到的数据程序会在下一次循环时尝试再次下载，直到3次都没有结果就放弃这个交易对的数据，这个思路写在DB_Local_utils_load.py中的函数run_function_list中。

脚本套件介绍：
GlobalObjects.py		中定义了常用的一些全局对象
DB_Local_utils_load.py	中定义了下载数据所用的所有对象
DB_Local_utils_Get.py	中定义了从下载好的数据库中批量提取数据的对象（数据库中已有数据的情况下才可使用）
database_fresh.py		用来创建一个新的数据库，并将fresh_symbols中的交易对数据存放入这个数据库中
database_update.py		用来将本地MySQL的指定数据库db_name_update中需要更新的交易对数据进行更新，更新的交易对存放在列表变量update_symbols中（数据库中已有数据的情况下才可使用）
print_all_symbols.py		生成symbols_usd
print_coins_picked_by_amt.py	生成coins_picked（数据库中已有日K线数据的情况下才可使用）
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
准备工作完成之后可以直接运行database_fresh.py脚本将交易额排名前10位的交易对1小时K线数据下载到新的数据库中，如果需要下载所有交易对的数据，那么将fresh_symbols赋值为GlobalObjects.py中的symbols_usd即可（或者自己修改）。
一段时间后若想更新数据库，注意定义好变量db_name_update、update_symbols、timeframe，然后使用database_update.py进行更新。
若需要批量提取数据字段，使用DB_Local_utils_Get.py中定义的对象即可。

