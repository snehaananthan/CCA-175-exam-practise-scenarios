crime = sc.textFile('/public/crime/csv')

header = crime.first()

from pyspark.sql import *
from pyspark.sql.functions import *

crime_DF = crime. \
filter(lambda c: c!=header). \
map(lambda c: 
	Row(month=str(c.split(',')[2][6:10]) + str(c.split(',')[2][0:2]), 
		crime_type=c.split(',')[5])
).toDF()

DF = crime_DF. \
groupBy('month', 'crime_type'). \
agg(count('crime_type').alias('crime_count')). \
orderBy('month', desc('crime_count')). \
select('month', 'crime_count', 'crime_type')

DF.rdd.map(tuple). \
map(lambda r: "\t".join(str(x) for x in r)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock3/problem6', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')