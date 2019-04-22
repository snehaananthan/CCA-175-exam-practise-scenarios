pyspark --master yarn \
--num-executors 10 \
--total-executor-cores 30 \
--executor-memory 1G \
--conf 'spark.ui.port=12999' \
--packages 'com.databricks:spark-avro_2.10:2.0.1'

text = sc.sequenceFile('/public/randomtextwriter')

from operator import add

rdd = text. \
repartition(30). \
map(lambda l: " ".join(l)). \
flatMap(lambda l: l.split(" ")). \
map(lambda w: (w.replace(",", "").replace(".", ""), 1)). \
reduceByKey(add)

for i in rdd.take(10): print(i)

from pyspark.sql import *

rdd.map(lambda (w, c): Row(word=w, count=int(c))). \
coalesce(8). \
write.format('com.databricks.spark.avro').save('/user/snehaananthan/mocks/mock1/problem8')