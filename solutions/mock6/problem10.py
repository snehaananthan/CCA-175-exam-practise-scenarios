products = sc.textFile('/user/snehaananthan/data/retail_db/products')

from pyspark.sql import *
from pyspark.sql.functions import *

products_DF = products. \
filter(lambda p: p.split(',')[4] != ''). \
map(lambda p:
	Row(product_id=int(p.split(',')[0]) ,
	 product_category_id=int(p.split(',')[1]) ,
	 product_name=p.split(',')[2] ,
	 product_description=p.split(',')[3] ,
	 product_price=float(p.split(',')[4]) ,
	 product_image=p.split(',')[5])
).toDF()

DF = products_DF. \
select('*', dense_rank().over(Window.orderBy(desc('product_price'))).alias('rank')). \
filter('rank <= 10'). \
orderBy(desc('product_price'), 'product_id'). \
select(products_DF['*'])

sqlContext.setConf('spark.sql.avro.compression.codec', 'uncompressed')

DF.write.format('com.databricks.spark.avro'). \
coalesce(1). \
save('/user/snehaananthan/mocks/mock6/problem10')