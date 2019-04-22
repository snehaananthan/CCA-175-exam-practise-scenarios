pyspark --master yarn \
--num-executors 4 \
--total-executor-cores 4 \
--executor-memory 512M \
--conf 'spark.ui.port=12999'

stocks = sc.textFile('/user/snehaananthan/data/nyse_stocks')

for i in stocks.take(10): print(i)

from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime


stocks_DF = stocks. \
map(lambda s: 
	Row(stock=s.split(',')[0],
		date=s.split(',')[1],
		date_month=(s.split(',')[1][:7]).replace('-', ''), 
		volume=int(s.split(',')[6]))
).toDF()

DF = stocks_DF. \
groupBy('date_month', 'stock'). \
agg(sum('volume').alias('volume')). \
select('*', dense_rank().over(Window.partitionBy('date_month').orderBy(desc('volume'))).alias('rank')). \
filter('rank <= 8'). \
orderBy('date_month', 'rank'). \
select('date_month', 'stock', 'volume')

DF.rdd.map(tuple). \
map(lambda d: "\t".join(str(x) for x in d)). \
coalesce(4). \
saveAsTextFile('/user/snehaananthan/mocks/mock1/problem6')