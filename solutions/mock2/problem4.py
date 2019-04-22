pyspark --master yarn \
--num-executors 4 \
--total-executor-cores 4 \
--executor-memory 512M \
--conf 'spark.ui.port=12999'

orders = sc.textFile('/user/snehaananthan/data/retail_db/orders')
orderItems = sc.textFile('/user/snehaananthan/data/retail_db/order_items')

from pyspark.sql import *
from pyspark.sql.functions import *

orders_DF = orders. \
map(lambda o: 
	Row(order_id=int(o.split(',')[0]),
		order_date=o.split(',')[1], 
		order_status=o.split(',')[3])
).toDF()

orderItems_DF = orderItems. \
map(lambda o: 
	Row(order_id=int(o.split(',')[1]),
		order_item_subtotal=float(o.split(',')[4]))
).toDF()

join_DF = orders_DF.join(orderItems_DF, 'order_id')

DF = join_DF. \
groupBy('order_date', 'order_status'). \
agg(sum('order_item_subtotal').alias('total_amount'), countDistinct('order_id').alias('total_orders')). \
select('*', to_date('order_date').alias('order_date_sorting')). \
orderBy(desc('order_date_sorting'), 'order_status', desc('total_amount'), 'total_orders'). \
select('order_date', 'order_status', 'total_orders', 'total_amount')


sqlContext.setConf('spark.sql.parquet.compression.codec', 'gzip')
DF.coalesce(4). \
write.parquet('/user/snehaananthan/mocks/mock2/problem4/parquet-gzip')

sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')
DF.coalesce(4). \
write.parquet('/user/snehaananthan/mocks/mock2/problem4/parquet-snappy')

DF.rdd.map(tuple). \
map(lambda r: ",".join(str(x) for x in r)). \
coalesce(4). \
saveAsTextFile('/user/snehaananthan/mocks/mock2/problem4/csv')