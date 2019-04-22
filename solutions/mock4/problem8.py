cust = sc.textFile('/user/snehaananthan/mocks/data/customer.txt')
cust_orders = sc.textFile('/user/snehaananthan/mocks/data/customer_orders.txt')

from pyspark.sql import *
from pyspark.sql.functions import *

cust_DF = cust. \
map(lambda c: 
	Row(id=int(c.split(',')[0]),
		name=c.split(',')[1], 
		age=int(c.split(',')[2]))
).toDF()

cust_orders_DF = cust_orders. \
map(lambda c: 
	Row(id=int(c.split(',')[0]),
		item=c.split(',')[1], 
		cost=float(c.split(',')[2]))
).toDF()

join_DF = cust_DF.join(cust_orders_DF, 'id')

DF = join_DF. \
groupBy('id', 'name'). \
agg(sum('cost').alias('revenue')). \
select('name', 'revenue')

DF.coalesce(1). \
write.json('/user/snehaananthan/mocks/mock4/problem8')