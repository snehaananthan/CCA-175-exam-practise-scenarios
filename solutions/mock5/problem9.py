pyspark --master yarn \
--num-executors 2 \
--total-executor-cores 2 \
--executor-memory 512M \
--conf 'spark.ui.port=12999' \
--packages 'com.databricks:spark-avro_2.10:2.0.1'

content = sc.textFile('/user/snehaananthan/mocks/data/Content.txt')

from pyspark.sql import *
from pyspark.sql.functions import *

DF = content. \
flatMap(lambda l: l.split(' ')). \
map(lambda w: (w, len(w))). \
map(lambda (w, l): Row(word=w, length=int(l))).toDF()

sqlContext.setConf('spark.sql.avro.compression.codec', 'snappy')

DF.filter('length between 1 and 4'). \
select('word', 'length'). \
coalesce(1). \
write.format('com.databricks.spark.avro').save('/user/snehaananthan/mocks/mock5/problem9')