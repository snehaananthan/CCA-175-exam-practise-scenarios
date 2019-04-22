crime = sc.textFile('/public/crime/csv')

header = crime.first()

from pyspark.sql import *
from pyspark.sql.functions import *

crime_DF = crime. \
filter(lambda c: c!=header). \
map(lambda c: 
	Row(crime_type=c.split(',')[5], 
		location=c.split(',')[7])
).toDF()

DF = crime_DF. \
filter('location = "RESIDENCE"'). \
groupBy('crime_type'). \
agg(count('crime_type').alias('number_of_incidents')). \
select('crime_type', 'number_of_incidents'). \
orderBy(desc('number_of_incidents')).limit(3)

DF.rdd.toDF(schema=['Crime Type', 'Number Of Incidents']). \
coalesce(1).write.json('/user/snehaananthan/mocks/mock3/problem7')



