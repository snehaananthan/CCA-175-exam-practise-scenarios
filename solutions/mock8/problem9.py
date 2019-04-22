DF = sqlContext.read.json('/user/snehaananthan/mocks/data/employee.json')

from pyspark.sql import *
from pyspark.sql.functions import *

DF.orderBy('first_name'). \
coalesce(1). \
write.json('/user/snehaananthan/mocks/mock8/problem9')