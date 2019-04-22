people = sc.textFile('/user/snehaananthan/mocks/data/people.txt') 

from pyspark.sql import *
from pyspark.sql.functions import *

people_DF = people. \
map(lambda p:
	Row(name=p.split(',')[0],
		age=float(p.split(',')[1]),
		gender=p.split(',')[2])
).toDF()

DF = people_DF. \
groupBy('gender'). \
agg(count('name').alias('number_of_people'), sum('age').alias('cumulative_ages'))

DF.rdd.map(tuple). \
map(lambda d: "\t".join(str(x) for x in d)). \
coalesce(1).saveAsTextFile('/user/snehaananthan/mocks/mock5/problem7', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')