patients = sc.textFile('/user/snehaananthan/mocks/data/patients.csv')

from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
	

patients_DF = patients. \
map(lambda p:
	Row(patientId=int(p.split(',')[0]) ,
		name=p.split(',')[1] ,
		dateOfBirth=p.split(',')[2] ,
		age=(datetime.today() - datetime.strptime(p.split(',')[2], '%Y-%m-%d')).days/365, 
		lastVisitDate=p.split(',')[3], 
		lvd_date=datetime.strptime(p.split(',')[3], '%Y-%m-%d'))
).toDF()

DF1 = patients_DF. \
filter((patients_DF.lvd_date < current_date()) & (patients_DF.lvd_date > to_date(lit('2012-09-15')))). \
select('patientId','name','dateOfBirth','lastVisitDate')

DF2 = patients_DF. \
filter(substring('dateOfBirth', 1, 4) == '2011'). \
select('patientId','name','dateOfBirth','lastVisitDate')

DF3 = patients_DF. \
select('patientId','name','dateOfBirth','lastVisitDate', 'age')

DF4 = patients_DF. \
filter(datediff(current_date(), patients_DF.lvd_date) > 60). \
select('patientId','name','dateOfBirth','lastVisitDate')

DF5 = patients_DF. \
filter('age <= 18'). \
select('patientId','name','dateOfBirth','lastVisitDate')

DF1.rdd.map(tuple). \
map(lambda d: ",".join(str(x) for x in d)). \
coalesce(1).saveAsTextFile('/user/snehaananthan/mocks/mock8/problem6/sol1')

DF2.rdd.map(tuple). \
map(lambda d: ",".join(str(x) for x in d)). \
coalesce(1).saveAsTextFile('/user/snehaananthan/mocks/mock8/problem6/sol2')

DF3.rdd.map(tuple). \
map(lambda d: ",".join(str(x) for x in d)). \
coalesce(1).saveAsTextFile('/user/snehaananthan/mocks/mock8/problem6/sol3')

DF4.rdd.map(tuple). \
map(lambda d: ",".join(str(x) for x in d)). \
coalesce(1).saveAsTextFile('/user/snehaananthan/mocks/mock8/problem6/sol4')

DF5.rdd.map(tuple). \
map(lambda d: ",".join(str(x) for x in d)). \
coalesce(1).saveAsTextFile('/user/snehaananthan/mocks/mock8/problem6/sol5')

DF2.rdd.map(tuple). \
map(lambda d: ",".join(str(x) for x in d)). \
saveAsTextFile('/user/snehaananthan/mocks/mock8/problem6/test')