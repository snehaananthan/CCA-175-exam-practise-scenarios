pyspark --master yarn \
--num-executors 4 \
--executor-memory 512M \
--total-executor-cores 4 \
--conf 'spark.ui.port=12999'


DF = sqlContext.read.json('/user/snehaananthan/mocks/data/employee.json')

DF.registerTempTable('mokka_table')

res_DF = sqlContext.sql('select * from mokka_table')

res_DF.toJSON(). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock6/problem4', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')