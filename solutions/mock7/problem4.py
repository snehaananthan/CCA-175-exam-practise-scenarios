pyspark --master yarn \
--num-executors 4 \
--executor-memory 512M \
--total-executor-cores 4 \
--conf 'spark.ui.port=12999'

products = sc.textFile('/user/snehaananthan/mocks/data/products.csv')

from pyspark.sql import *
from pyspark.sql.functions import *

products_DF = products. \
map(lambda p:
	Row(productId=int(p.split(',')[0]) ,
		productCode=p.split(',')[1] ,
		name=p.split(',')[2],
		quantity=int(p.split(',')[3]),
		price=float(p.split(',')[4]),
		supplierId=int(p.split(',')[5]))
).toDF()

products_DF.registerTempTable('temp_mock7_prob4')

sqlContext.sql('create table snehaananthan_retail_db_txt.products_orc stored as orc as select * from temp_mock7_prob4')
sqlContext.sql('create table snehaananthan_retail_db_txt.products_parquet stored as parquet as select * from temp_mock7_prob4')

