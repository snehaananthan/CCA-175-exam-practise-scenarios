orders = sc.textFile('/user/snehaananthan/data/retail_db/orders')
orderItems = sc.textFile('/user/snehaananthan/data/retail_db/order_items')

from pyspark.sql import *
from pyspark.sql.functions import *

orders_DF = orders. \
map(lambda o:
	Row(order_id=int(o.split(',')[0]),
		order_date=o.split(',')[1])
).toDF()

orderItems_DF = orderItems. \
map(lambda o:
	Row(order_id=int(o.split(',')[1]),
		order_item_subtotal=float(o.split(',')[4]))
).toDF()

join_DF = orders_DF.join(orderItems_DF, 'order_id')

DF1 = join_DF. \
groupBy('order_id', 'order_date'). \
agg(sum('order_item_subtotal').alias('amount'))

DF2 = orders_DF. \
groupBy('order_date'). \
agg(count('order_id').alias('total_orders')). \
select('*', to_date(substring('order_date', 1, 10)).alias('order_date_date')). \
orderBy('order_date_date'). \
select('order_date', 'total_orders')

DF1.coalesce(1).write.json('/user/snehaananthan/mocks/mock6/problem5/sol1')
DF2.rdd.map(tuple). \
map(lambda r: ",".join(str(x) for x in r)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock6/problem5/sol2', compressionCodecClass='org.apache.hadoop.io.compress.SnappyCodec')

