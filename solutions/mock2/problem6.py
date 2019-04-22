sqoop import --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 4 \
--table 'orders' \
--target-dir '/user/snehaananthan/mocks/mock2/problem6/avro' \
--as-avrodatafile \
--outdir 'java'

pyspark --master yarn \
--num-executors 4 \
--executor-memory 512M \
--total-executor-cores 4 \
--conf 'spark.ui.port=12999' \
--packages 'com.databricks:spark-avro_2.10:2.0.1'

DF = sqlContext.read.format('com.databricks.spark.avro').load('/user/snehaananthan/mocks/mock2/problem6/avro')

sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')
DF.coalesce(4). \
write.parquet('/user/snehaananthan/mocks/mock2/problem6/parquet-snappy-compress')

DF.rdd.map(tuple). \
map(lambda r: " ".join(str(x) for x in r)). \
saveAsTextFile('/user/snehaananthan/mocks/mock2/problem6/text-gzip-compress', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')

DF.rdd.map(tuple). \
map(lambda r: (r[0], " ".join(str(x) for x in r))). \
saveAsSequenceFile('/user/snehaananthan/mocks/mock2/problem6/sequence')

DF.rdd.map(tuple). \
map(lambda r: ",".join(str(x) for x in r)). \
saveAsTextFile('/user/snehaananthan/mocks/mock2/problem6/text-snappy-compress', compressionCodecClass='org.apache.hadoop.io.compress.SnappyCodec')