sqoop import --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 4 \
--table 'orders' \
--target-dir '/user/snehaananthan/mocks/mock2/problem9/text' \
--fields-terminated-by '\t' \
--outdir 'java'


pyspark --master yarn \
--num-executors 4 \
--total-executor-cores 4 \
--executor-memory 512M \
--conf 'spark.ui.port=12999' \
--packages 'com.databricks:spark-avro_2.10:2.0.1'

DF = sqlContext.read.parquet('/user/snehaananthan/mocks/mock2/problem6/parquet-snappy-compress')

sqlContext.setConf('spark.sql.parquet.compression.codec', 'uncompressed') 
DF.write.parquet('/user/snehaananthan/mocks/mock2/problem9/parquet-no-compress')

sqlContext.setConf('spark.sql.avro.compression.codec', 'snappy')
DF.write.format('com.databricks.spark.avro').save('/user/snehaananthan/mocks/mock2/problem9/avro-snappy')