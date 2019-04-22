patients = sc.textFile('/user/snehaananthan/mocks/data/patients.csv')

from pyspark.sql import *
from pyspark.sql.functions import *
	

patients_DF = patients. \
map(lambda p:
	Row(patientId=int(p.split(',')[0]) ,
		name=p.split(',')[1] ,
		dateOfBirth=p.split(',')[2] ,
		lastVisitDate=p.split(',')[3])
).toDF()

patients_DF. \
coalesce(1). \
write.format('com.databricks.spark.avro').save('/user/snehaananthan/mocks/mock8/problem7/data')

