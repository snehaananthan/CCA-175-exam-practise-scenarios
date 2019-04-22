pyspark --master yarn \
--num-executors 4 \
--total-executor-cores 4 \
--executor-memory 512M \
--conf 'spark.ui.port=12999'

orderItems = sc.textFile('/user/snehaananthan/data/retail_db/order_items')

from pyspark.sql import *
from pyspark.sql.functions import *

orderItems_DF = orderItems. \
map(lambda o: 
	Row(order_id=int(o.split(',')[1]), 
		order_item_subtotal=float(o.split(',')[4]))
).toDF()

DF = orderItems_DF. \
groupBy('order_id'). \
agg(sum('order_item_subtotal').alias('total_rev'), 
	max('order_item_subtotal').alias('max_rev'), 
	min('order_item_subtotal').alias('min_rev'), 
	avg('order_item_subtotal').alias('avg_rev'))

DF.toJSON(). \
coalesce(4). \
saveAsTextFile('/user/snehaananthan/mocks/mock6/problem6', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')