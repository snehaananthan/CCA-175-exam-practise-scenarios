pyspark --master yarn \
--num-executors 4 \
--total-executor-cores 4 \
--executor-memory 512M \
--conf 'spark.ui.port=12999' \
--packages 'com.databricks:spark-avro_2.10:2.0.1'

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
groupBy('order_date', 'order_id'). \
agg(sum('order_item_subtotal').alias('total_revenue'))

DF2_inter = join_DF. \
groupBy('order_date', 'order_id'). \
agg(sum('order_item_subtotal').alias('order_revenue'))

DF2 = DF2_inter. \
groupBy('order_date'). \
agg(sum('order_revenue').alias('total_revenue'), 
	avg('order_revenue').alias('avg_revenue'))

DF1.coalesce(4).write.orc('/user/snehaananthan/mocks/mock6/problem8/sol1')
DF2.coalesce(4).write.format('com.databricks.spark.avro'). \
save('/user/snehaananthan/mocks/mock6/problem8/sol2')