
pyspark --master yarn \
--num-executors 2 \
--total-executor-cores 2 \
--executor-memory 512M \
--conf 'spark.ui.port=12999'

name = sc.textFile('/user/snehaananthan/mocks/data/EmployeeName.csv')

from pyspark.sql import *
from pyspark.sql.functions import *

DF = name. \
map(lambda l: 
	Row(id=int(l.split(',')[0]), 
		name=l.split(',')[1])
).toDF()

DF.orderBy('name'). \
select('name', 'id'). \
rdd.map(tuple). \
map(lambda r: " ".join(str(x) for x in r)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock4/problem5')