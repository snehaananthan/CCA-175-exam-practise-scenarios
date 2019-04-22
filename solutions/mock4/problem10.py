sales = sc.textFile('/user/snehaananthan/mocks/data/sales.txt')

from pyspark.sql import *
from pyspark.sql.functions import *

sales_DF = sales. \
map(lambda s:
	Row(dept=s.split(',')[0],
		desgn=s.split(',')[1], 
		state=s.split(',')[2],
		sal=float(s.split(',')[3]))
).toDF()

DF = sales_DF. \
groupBy('dept', 'desgn', 'state'). \
agg(sum('sal').alias('costToCompany'), count('sal').alias('totalEmployeeCount'))

DF.rdd.map(tuple). \
map(lambda d: ",".join(str(x) for x in d)). \
coalesce(1).saveAsTextFile('/user/snehaananthan/mocks/mock4/problem10')