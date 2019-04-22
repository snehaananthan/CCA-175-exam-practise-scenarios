pyspark --master yarn \
--num-executors 4 \
--executor-memory 1G \
--total-executor-cores 4 \
--conf 'spark.ui.port=13999'

categories = sc.textFile('/user/snehaananthan/data/retail_db/categories')
orderItems = sc.textFile('/user/snehaananthan/data/retail_db/order_items')
departments = sc.textFile('/user/snehaananthan/data/retail_db/departments')
products = sc.textFile('/user/snehaananthan/data/retail_db/products')

from pyspark.sql import *
from pyspark.sql.functions import *

categories_DF = categories. \
map(lambda c:
	Row(category_id=int(c.split(',')[0]), 
		category_department_id=int(c.split(',')[1]))
).toDF()

orderItems_DF = orderItems. \
map(lambda o: 
	Row(order_item_product_id=int(o.split(',')[2]), 
		order_item_subtotal=float(o.split(',')[4]))
).toDF()

departments_DF = departments. \
map(lambda d:
	Row(department_id=int(d.split(',')[0]), 
		department_name=d.split(',')[1])
).toDF()

products_DF = products. \
map(lambda p:
	Row(product_id=int(p.split(',')[0]), 
		product_category_id=int(p.split(',')[1]))
).toDF()

join_DF = categories_DF. \
join(products_DF, categories_DF.category_id == products_DF.product_category_id). \
join(orderItems_DF, orderItems_DF.order_item_product_id == products_DF.product_id). \
join(departments_DF, departments_DF.department_id == categories_DF.category_department_id)

DF1 = join_DF. \
groupBy('department_id', 'department_name', 'category_id'). \
agg(sum('order_item_subtotal').alias('revenue')). \
select('department_name', 'category_id', 'revenue', dense_rank().over(Window.partitionBy('department_id').orderBy(desc('revenue'))).alias('rank')). \
orderBy('department_name', 'rank')

DF2_inter = join_DF. \
groupBy('department_id', 'department_name', 'category_id'). \
agg(sum('order_item_subtotal').alias('revenue'))

DF2 = DF2_inter. \
select('department_name', 'category_id', 'revenue', round(((DF2_inter.revenue/sum('revenue').over(Window.partitionBy('department_id')))*100), 2).alias('percentage'))

DF1.coalesce(1).write.orc('/user/snehaananthan/mocks/mock1/problem4/sol1')
DF2.coalesce(1).write.orc('/user/snehaananthan/mocks/mock1/problem4/sol2')