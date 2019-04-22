pyspark --master yarn \
--num-executors 4 \
--total-executor-cores 4 \
--executor-memory 512M \
--conf 'spark.ui.port=12999' \
--packages 'com.databricks:spark-avro_2.10:2.0.1'

products = sc.textFile('/user/snehaananthan/mocks/data/products.csv')
suppliers = sc.textFile('/user/snehaananthan/mocks/data/suppliers.csv')

from pyspark.sql import *
from pyspark.sql.functions import *

pro_DF = products. \
map(lambda p:
	Row(product_name=p.split(',')[2],
		price=float(p.split(',')[4]),
		supplier_id=int(p.split(',')[5]))
).toDF()

sup_DF = suppliers. \
map(lambda s:
	Row(supplier_id=int(s.split(',')[0]),
		supplier_name=s.split(',')[1])
).toDF()

DF = pro_DF.join(sup_DF, 'supplier_id'). \
filter('price < 0.6'). \
select('product_name', 'price', 'supplier_name')


sqlContext.setConf('spark.sql.avro.compression.codec', 'snappy')
DF.coalesce(1). \
write.format('com.databricks.spark.avro').save('/user/snehaananthan/mocks/mock8/problem4')

