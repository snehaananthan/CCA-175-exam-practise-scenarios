pyspark --master yarn \
--num-executors 2 \
--executor-memory 512M \
--total-executor-cores 4 \
--conf 'spark.ui.port=12999'


orders = sc.textFile('/user/snehaananthan/data/retail_db/orders')
customers = sc.textFile('/user/snehaananthan/data/retail_db/customers')

from pyspark.sql import *
from pyspark.sql.functions import *

orders_DF = orders. \
map(lambda o:
	Row(order_id=int(o.split(',')[0]),
		customer_id=int(o.split(',')[2]))
).toDF()

customers_DF = customers. \
map(lambda o:
	Row(customer_id=int(o.split(',')[0]), 
		customer_fname=o.split(',')[1],
		customer_lname=o.split(',')[2])
).toDF()

DF = orders_DF.join(customers_DF, 'customer_id', 'right_outer'). \
filter('order_id is null'). \
select('customer_lname', 'customer_fname'). \
orderBy('customer_lname', 'customer_fname')

DF.rdd.map(tuple). \
map(lambda d: ", ".join(str(x) for x in d)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock1/problem9')