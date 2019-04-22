pyspark --master yarn \
--num-executors 4 \
--executor-memory 512M \
--total-executor-cores 4 \
--conf 'spark.ui.port=12999'

products = sc.textFile('/user/snehaananthan/data/retail_db/products')
categories = sc.textFile('/user/snehaananthan/data/retail_db/categories')
departments= sc.textFile('/user/snehaananthan/data/retail_db/departments')

from pyspark.sql import *
from pyspark.sql.functions import *

products_DF = products. \
filter(lambda p: p.split(',')[4] != '') . \
map(lambda p:
	Row(product_id=int(p.split(',')[0]), 
		category_id=int(p.split(',')[1]), 
		product_name=p.split(',')[2], 
		product_description=p.split(',')[3], 
		product_price=float(p.split(',')[4]), 
		product_image=p.split(',')[5])
).toDF()

categories_DF = categories. \
map(lambda c:
	Row(category_id=int(c.split(',')[0]),
		department_id=int(c.split(',')[1]))
).toDF()

departments_DF = departments. \
map(lambda p:
	Row(department_id=int(p.split(',')[0]), 
		department_name=p.split(',')[1])
).toDF()

join_DF = products_DF. \
join(categories_DF, 'category_id'). \
join(departments_DF, 'department_id')

DF = join_DF. \
filter('product_price < 100.00'). \
select('department_name', rank().over(Window.partitionBy('department_name').orderBy(desc('product_price'))).alias('rank'), products_DF['*']). \
orderBy('department_name', desc('rank'))

DF.registerTempTable('temp_mock2_problem7')

sqlContext.sql('create table snehaananthan_retail_db_txt.mock2_problem7 as select * from temp_mock2_problem7')
