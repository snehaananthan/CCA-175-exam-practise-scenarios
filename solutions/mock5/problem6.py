work = sc.textFile('/user/snehaananthan/mocks/data/workdone.txt') 

from pyspark.sql import *
from pyspark.sql.functions import *

work_DF = work. \
map(lambda w: 
	Row(name=w.split(',')[0],
		sex=w.split(',')[1],
		salary=float(w.split(',')[2]))
).toDF()

DF = work_DF. \
groupBy('name', 'sex'). \
agg(sum('salary').alias('sum_of_cost'))

sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')
DF.coalesce(1).write.parquet('/user/snehaananthan/mocks/mock5/problem6')