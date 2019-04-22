nyse = sc.textFile('/user/snehaananthan/data/nyse')

from pyspark.sql import *
from pyspark.sql.functions import *

DF = nyse. \
map(lambda n: 
	Row(stockticker=n.split(',')[0], 
		transactiondate=n.split(',')[1], 
		openprice=float(n.split(',')[2]), 
		highprice=float(n.split(',')[3]), 
		lowprice=float(n.split(',')[4]),
		closeprice=float(n.split(',')[5]), 
		volume=int(n.split(',')[6]))
).toDF(). \
select('stockticker', 'transactiondate', 'openprice', 'highprice', 'lowprice', 'closeprice', 'volume')

sqlContext.setConf('spark.sql.parquet.compression.codec', 'uncompressed')
DF.write.parquet('/user/snehaananthan/mocks/mock3/problem8')