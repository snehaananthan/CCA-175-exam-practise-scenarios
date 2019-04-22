course = sc.textFile('/user/snehaananthan/mocks/data/course.txt')
fee = sc.textFile('/user/snehaananthan/mocks/data/fee.txt')

from pyspark.sql import *
from pyspark.sql.functions import *

course_DF = course. \
map(lambda c:
	Row(id=int(c.split(',')[0]),
		course=c.split(',')[1])
).toDF()

fee_DF = fee. \
map(lambda c:
	Row(id=int(c.split(',')[0]),
		fee=float(c.split(',')[1]))
).toDF()


DF1 = course_DF.join(fee_DF, 'id', 'left_outer')

DF2 = course_DF.join(fee_DF, 'id', 'right_outer')

DF3 = course_DF.join(fee_DF, 'id', 'full_outer'). \
filter('fee is not null')

DF1.rdd.map(tuple). \
map(lambda d: " ".join(str(x) for x in d)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock8/problem8/sol1', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')

DF2.rdd.map(tuple). \
map(lambda d: " ".join(str(x) for x in d)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock8/problem8/sol2', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')

DF3.rdd.map(tuple). \
map(lambda d: " ".join(str(x) for x in d)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock8/problem8/sol3', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')