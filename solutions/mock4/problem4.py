pyspark --master yarn \
--num-executors 2 \
--total-executor-cores 2 \
--executor-memory 512M \
--conf 'spark.ui.port=12999'

name = sc.textFile('/user/snehaananthan/mocks/data/EmployeeName.csv')
sal = sc.textFile('/user/snehaananthan/mocks/data/EmployeeSalary.csv')

from pyspark.sql import *
from pyspark.sql.functions import *

name_DF = name. \
map(lambda n: 
	Row(id=int(n.split(',')[0]), 
		name=n.split(',')[1])
).toDF()

sal_DF = sal. \
map(lambda n: 
	Row(id=int(n.split(',')[0]), 
		sal=float(n.split(',')[1]))
).toDF()

name_DF.join(sal_DF, 'id'). \
orderBy('sal', 'name'). \
select('name', 'sal'). \
rdd.map(tuple). \
map(lambda r: (r[0], r[1])). \
saveAsSequenceFile('/user/snehaananthan/mocks/mock4/problem4', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')

