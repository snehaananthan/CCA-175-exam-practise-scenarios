pyspark --master yarn \
--num-executors 4 \
--executor-memory 512M \
--total-executor-cores 4 \
--conf 'spark.ui.port=12999' \
--packages 'com.databricks:spark-avro_2.10:2.0.1'

products = sc.textFile('/user/snehaananthan/data/retail_db/products')

from pyspark.sql import *
from pyspark.sql.functions import *

products_DF = products. \
filter(lambda p: p.split(',')[4] != ''). \
map(lambda p:
	Row(product_id=int(p.split(',')[0]),
		product_category_id=int(p.split(',')[1]),
		product_name=p.split(',')[2],
	    product_price=float(p.split(',')[4]))
).toDF(). \
filter('product_price < 100.00')


highest_priced_product = products_DF. \
select('product_category_id', products_DF.product_price.alias('highest_price'), \
	dense_rank().over(Window.partitionBy('product_category_id').orderBy(desc('product_price'), 'product_id')).alias('rank')). \
filter('rank = 1').drop('rank')

lowest_priced_product = products_DF. \
select('product_category_id', products_DF.product_price.alias('lowest_price'), \
	dense_rank().over(Window.partitionBy('product_category_id').orderBy('product_price', 'product_id')).alias('rank')). \
filter('rank = 1').drop('rank')

DF = products_DF. \
groupBy('product_category_id'). \
agg(count('product_id').alias('total_products'), avg('product_price').alias('avg_price')). \
join(highest_priced_product, 'product_category_id'). \
join(lowest_priced_product, 'product_category_id'). \
orderBy('product_category_id')

sqlContext.setConf('spark.sql.avro.compression.codec', 'snappy')
DF.coalesce(4). \
write.format('com.databricks.spark.avro').save('/user/snehaananthan/mocks/mock2/problem5')
