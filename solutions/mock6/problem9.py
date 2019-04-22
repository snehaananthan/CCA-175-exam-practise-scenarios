orders = sc.textFile('/user/snehaananthan/data/retail_db/orders')
orderItems = sc.textFile('/user/snehaananthan/data/retail_db/order_items')

from pyspark.sql import *
from pyspark.sql.functions import *

orders_DF = orders. \
map(lambda o:
	Row(order_id=int(o.split(',')[0]),
		order_date=o.split(',')[1],
		customer_id=int(o.split(',')[2]))
).toDF()


orderItems_DF = orderItems. \
map(lambda o:
	Row(order_id=int(o.split(',')[0]),
		item_subtotal=float(o.split(',')[4]))
).toDF()

join_DF = orders_DF.join(orderItems_DF, 'order_id')

DF1 = join_DF. \
groupBy('order_date', 'customer_id'). \
agg(sum('item_subtotal').alias('total_revenue'))

DF2 = join_DF. \
groupBy('customer_id', 'order_id'). \
agg(sum('item_subtotal').alias('order_revenue')). \
select('*', dense_rank().over(Window.partitionBy('customer_id').orderBy(desc('order_revenue'))).alias('rank')). \
filter('rank = 1'). \
select('customer_id', 'order_id')

DF1.toJSON(). \
coalesce(3). \
saveAsTextFile('/user/snehaananthan/mocks/mock6/problem9/sol1', compressionCodecClass='org.apache.hadoop.io.compress.SnappyCodec')

DF2.rdd.map(tuple). \
map(lambda r: "|".join(str(x) for x in r)). \
coalesce(4). \
saveAsTextFile('/user/snehaananthan/mocks/mock6/problem9/sol2', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')