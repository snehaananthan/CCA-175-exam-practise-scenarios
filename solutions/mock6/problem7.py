orders = sc.textFile('/user/snehaananthan/data/retail_db/orders')

from pyspark.sql import *
from pyspark.sql.functions import *

orders_DF = orders. \
map(lambda o:
	Row(order_id=int(o.split(',')[0]),
		order_status=o.split(',')[3])
).toDF()

DF = orders_DF. \
groupBy('order_status'). \
agg(count('order_id').alias('num_of_orders'))

DF.rdd.map(tuple). \
map(lambda d: "|".join(str(x) for x in d)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock6/problem7', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')

