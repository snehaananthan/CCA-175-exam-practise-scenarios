pyspark --master yarn \
--num-executors 4 \
--executor-memory 512M \
--total-executor-cores 4 \
--conf 'spark.ui.port=12999'

name = sc.textFile('/user/snehaananthan/mocks/data/EmployeeName.csv')
manager = sc.textFile('/user/snehaananthan/mocks/data/EmployeeManager.csv')
salary = sc.textFile('/user/snehaananthan/mocks/data/EmployeeSalary.csv')

from pyspark.sql import *
from pyspark.sql.functions import *

name_DF = name. \
map(lambda n: 
	Row(id=int(n.split(',')[0]),
		name=n.split(',')[1])
).toDF()

manager_DF = manager. \
map(lambda n: 
	Row(id=int(n.split(',')[0]),
		managerName=n.split(',')[1])
).toDF()

salary_DF = salary. \
map(lambda n: 
	Row(id=int(n.split(',')[0]),
		salary=float(n.split(',')[1]))
).toDF()

DF = name_DF.join(manager_DF, 'id').join(salary_DF, 'id'). \
orderBy('id'). \
select(concat_ws('-', 'name', 'id').alias('name-id'), 'salary', 'managerName')

DF.rdd.map(tuple). \
map(lambda d: ",".join(str(x) for x in d)). \
saveAsTextFile('/user/snehaananthan/mocks/mock3/problem4')
