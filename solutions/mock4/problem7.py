pyspark --master yarn \
--num-executors 2 \
--total-executor-cores 2 \
--executor-memory 512M \
--conf 'spark.ui.port=12999' \
--packages 'com.databricks:spark-avro_2.10:2.0.1'

feedback = sc.textFile('/user/snehaananthan/mocks/data/feedback.txt')

for i in feedback.collect(): print(i)

from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime

DF = feedback. \
map(lambda f: 
	Row(name=f.split('|')[0].strip(), 
		date=f.split('|')[1].strip(),
		date_format=datetime.strptime(f.split('|')[1].strip(), '%b %d, %Y'), 
		rating=f.split('|')[2].strip())
).toDF()

sqlContext.setConf('spark.sql.avro.compression.codec', 'uncompressed')

DF.orderBy('date_format'). \
select('name', 'date', 'rating'). \
coalesce(1). \
write.format('com.databricks.spark.avro').save('/user/snehaananthan/mocks/mock4/problem7')

