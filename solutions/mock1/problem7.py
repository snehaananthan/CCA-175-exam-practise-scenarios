pyspark --master yarn \
--num-executors 4 \
--total-executor-cores 4 \
--executor-memory 512M \
--conf 'spark.ui.port=12999'

employees = sc.textFile('/user/snehaananthan/data/hr_db/employees')

for i in employees.take(10): print(i)

from pyspark.sql import *
from pyspark.sql.functions import *

employees_DF = employees. \
map(lambda e: 
	Row(first_name=e.split('\t')[1],
		last_name=e.split('\t')[2], 
		salary=float(e.split('\t')[7]),
		commission_pct=float(e.split('\t')[8]) if e.split('\t')[8] != 'null' else None)
).toDF()

DF = employees_DF. \
withColumn('commission_earned', (employees_DF.salary * employees_DF.commission_pct).cast('string')). \
fillna({'commission_earned':'N/E'}). \
orderBy('first_name', 'last_name'). \
select('first_name', 'last_name', 'salary', 'commission_earned')


DF.rdd.map(tuple). \
map(lambda r: " ".join(str(x) for x in r)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock1/problem7')