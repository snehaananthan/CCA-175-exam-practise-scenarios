pyspark --master yarn \
--num-executors 4 \
--total-executor-cores 4 \
--executor-memory 512M \
--conf 'spark.ui.port=12999' \
--packages 'com.databricks:spark-avro_2.10:2.0.1'

data_DF = sqlContext.sql('select * from snehaananthan_retail_db_txt.products_parquet')

DF1 = data_DF. \
select('name', 'quantity'). \
filter('quantity <= 2000') 

DF2 = data_DF. \
filter('productCode = "PEN"'). \
select('name', 'price')

DF3 = data_DF. \
filter('name like "pencil%"'). \
select('productId','productCode','name','quantity','price','supplierId')

DF1.write.format('com.databricks.spark.avro').save('/user/snehaananthan/mocks/mock7/problem5/sol1')
DF2.write.format('com.databricks.spark.avro').save('/user/snehaananthan/mocks/mock7/problem5/sol2')
DF3.write.format('com.databricks.spark.avro').save('/user/snehaananthan/mocks/mock7/problem5/sol3')