orders = sc.textFile('/user/snehaananthan/data/retail_db/orders')
orderItems = sc.textFile('/user/snehaananthan/data/retail_db/order_items')
customers = sc.textFile('/user/snehaananthan/data/retail_db/customers')

from pyspark.sql import *
from pyspark.sql.functions import *

orders_DF = orders. \
map(lambda o: 
	Row(order_id=int(o.split(',')[0]),
		customer_id=int(o.split(',')[2]))
).toDF()

orderItems_DF = orderItems. \
map(lambda oi:
	Row(order_id=int(oi.split(',')[1]), 
		product_id=int(oi.split(',')[2]), 
		product_price=float(oi.split(',')[5]))
).toDF()

customers_DF = customers. \
map(lambda c:
	Row(customer_id=int(c.split(',')[0]),
	 customer_fname=c.split(',')[1],
	 customer_lname=c.split(',')[2],
	 customer_email=c.split(',')[3],
	 customer_password=c.split(',')[4],
	 customer_street=c.split(',')[5],
	 customer_city=c.split(',')[6],
	 customer_state=c.split(',')[7],
	 customer_zipcode=c.split(',')[8])
).toDF()

join_DF = orders_DF.join(orderItems_DF, 'order_id')

DF = join_DF. \
filter('product_price < 100.00'). \
groupBy('customer_id'). \
agg(countDistinct('product_id').alias('unique_product_purchases')). \
select('*', rank().over(Window.partitionBy('unique_product_purchases').orderBy('customer_id')).alias('rank')). \
filter('rank = 1'). \
orderBy(desc('unique_product_purchases')). \
limit(10)

result_DF = DF. \
join(customers_DF, DF.customer_id == customers_DF.customer_id). \
select(customers_DF.customer_id, 'customer_fname', 'customer_lname', 'customer_email', 'customer_password', 'customer_street', 'customer_city', 'customer_state', 'customer_zipcode')

result_DF.registerTempTable('temp_mock2_problem8')

sqlContext.sql('create table snehaananthan_retail_db_txt.mock2_problem8 as select * from temp_mock2_problem8')