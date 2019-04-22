DF1 = sqlContext.read.format('com.databricks.spark.avro').load('/user/snehaananthan/mocks/mock2/problem9/avro-snappy')

DF1.toJSON().saveAsTextFile('/user/snehaananthan/mocks/mock2/problem10/json-no-compress')

DF1.toJSON().saveAsTextFile('/user/snehaananthan/mocks/mock2/problem10/json-gzip', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')

DF2 = sqlContext.read.json('/user/snehaananthan/mocks/mock2/problem10/json-gzip')

DF2.rdd.map(tuple). \
map(lambda d: ",".join(str(x) for x in d)). \
coalesce(4). \
saveAsTextFile('/user/snehaananthan/mocks/mock2/problem10/csv-gzip', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')

rdd = sc.sequenceFile('/user/snehaananthan/mocks/mock2/problem6/sequence')

DF = rdd. \
map(lambda r: r[1].split(' ')). \
toDF(schema=['order_id', 'order_date', 'order_customer_id', 'order_status'])

DF.write.orc('/user/snehaananthan/mocks/mock2/problem10/orc')