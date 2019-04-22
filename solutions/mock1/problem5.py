categories = sc.textFile('/user/snehaananthan/data/retail_db/categories')
orderItems = sc.textFile('/user/snehaananthan/data/retail_db/order_items')
departments = sc.textFile('/user/snehaananthan/data/retail_db/departments')
products = sc.textFile('/user/snehaananthan/data/retail_db/products')
orders = sc.textFile('/user/snehaananthan/data/retail_db/orders')

from pyspark.sql import *
from pyspark.sql.functions import *

orders_DF = orders. \
map(lambda o:
	Row(order_id=int(o.split(',')[0]),
		order_date=o.split(',')[1],
		order_status=o.split(',')[3])
).toDF()

orderItems_DF = orderItems. \
map(lambda oi:
	Row(order_id=int(oi.split(',')[1]),
		product_id=int(oi.split(',')[2]),
		order_item_subtotal=float(oi.split(',')[4]))
).toDF()

products_DF = products. \
map(lambda p:
	Row(product_id=int(p.split(',')[0]),
		category_id=int(p.split(',')[1]))
).toDF()

categories_DF = categories. \
map(lambda c:
	Row(category_id=int(c.split(',')[0]),
		department_id=int(c.split(',')[1]))
).toDF()

departments_DF = departments. \
map(lambda d:
	Row(department_id=int(d.split(',')[0]), 
		department_name=d.split(',')[1])
).toDF()

join_DF = orders_DF. \
join(orderItems_DF, 'order_id'). \
join(products_DF, 'product_id'). \
join(categories_DF, 'category_id'). \
join(departments_DF, 'department_id')

DF = join_DF. \
filter("order_status IN ('COMPLETE', 'CLOSED')"). \
groupBy('order_date', 'department_id', 'department_name'). \
agg(sum('order_item_subtotal').alias('order_revenue')). \
select('order_date', 'department_name', 'order_revenue'). \
orderBy('order_date', 'department_name', 'order_revenue')

DF.rdd.map(tuple). \
map(lambda d: ",".join(str(x) for x in d)). \
coalesce(2). \
saveAsTextFile('/user/snehaananthan/mocks/mock1/problem5')
