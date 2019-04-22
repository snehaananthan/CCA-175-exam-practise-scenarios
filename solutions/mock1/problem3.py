pyspark --master yarn \
--num-executors 4 \
--executor-memory 512M \
--total-executor-cores 4 \
--conf 'spark.ui.port=13999'

election = sc.textFile('/user/snehaananthan/mocks/data/electionresults.txt')

header = election.first()

rdd = election.filter(lambda e: e!=header and e.split('\t')[0] != '')

from pyspark.sql import *
from pyspark.sql.functions import *

election_DF = rdd. \
map(lambda e: 
	Row(state=e.split('\t')[0], 
		const=e.split('\t')[1],
		partyname=e.split('\t')[6])
).toDF()

UP_DF = election_DF. \
filter('state = "Uttar Pradesh"')

BJP_BSP_DF = UP_DF. \
filter('partyname = "BJP" or partyname = "BSP"'). \
groupBy('partyname'). \
agg(count('const').alias('number_of_seats')). \
select('partyname', 'number_of_seats')

INC_SP_DF = UP_DF. \
filter('partyname = "INC" or partyname = "SP"'). \
agg(count('const').alias('number_of_seats')). \
withColumn('partyname', lit('INC+SP')). \
select('partyname', 'number_of_seats')

DF = BJP_BSP_DF.unionAll(INC_SP_DF)

DF.rdd.toDF(schema=['partyname', 'number of seats']). \
coalesce(1). \
write.json('/user/snehaananthan/mocks/mock1/problem3')
