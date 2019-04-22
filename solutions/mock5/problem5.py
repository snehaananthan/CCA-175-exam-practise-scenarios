tech = sc.textFile('/user/snehaananthan/mocks/data/technology.txt')
sal = sc.textFile('/user/snehaananthan/mocks/data/salary.txt')

from pyspark.sql import *
from pyspark.sql.functions import *

tech_DF = tech. \
map(lambda t: 
	Row(first=t.split(',')[0],
		last=t.split(',')[1],
		tech=t.split(',')[2])
).toDF()

sal_DF = sal. \
map(lambda t: 
	Row(first=t.split(',')[0],
		last=t.split(',')[1],
		sal=float(t.split(',')[2]))
).toDF()

DF = tech_DF.join(sal_DF, ['first', 'last']). \
select(concat_ws(' ', 'first', 'last').alias('name'), 'tech', 'sal')

DF.rdd.map(tuple). \
map(lambda d: " | ".join(str(x) for x in d)). \
coalesce(1).saveAsTextFile('/user/snehaananthan/mocks/mock5/problem5', compressionCodecClass='org.apache.hadoop.io.compress.SnappyCodec')