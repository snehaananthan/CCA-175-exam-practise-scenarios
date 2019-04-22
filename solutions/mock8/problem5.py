pyspark --master yarn \
--num-executors 4 \
--executor-memory 512M \
--total-executor-cores 4 \
--conf 'spark.ui.port=12999' \
--packages 'com.databricks:spark-avro_2.10:2.0.1'

products = sc.textFile('/user/snehaananthan/mocks/data/products.csv')
suppliers = sc.textFile('/user/snehaananthan/mocks/data/suppliers.csv')

from pyspark.sql import *
from pyspark.sql.functions import *

pro_DF = products. \
map(lambda p:
	Row(productId=int(p.split(',')[0]),
		productCode=p.split(',')[1],
		productName=p.split(',')[2],
		quantity=int(p.split(',')[3]),
		price=float(p.split(',')[4]),
		supplierId=int(p.split(',')[5]))
).toDF()

sup_DF = suppliers. \
map(lambda p:
	Row(supplierId=int(p.split(',')[0]),
		supplierName=p.split(',')[1])
).toDF()

join_DF = pro_DF.join(sup_DF, 'supplierId')

DF1 = join_DF. \
select('supplierId', 'productName', 'price'). \
orderBy('supplierId', 'productName')

DF2 = join_DF. \
filter('productName = "pencil 3B"'). \
select('supplierName').distinct()

DF3 = join_DF. \
filter('supplierName = "ABC Traders"'). \
select('productId','productCode','productName','quantity','price','supplierId')

DF1.rdd.map(tuple). \
map(lambda d: "\t".join(str(x) for x in d)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock8/problem5/sol1')

DF2.rdd.map(tuple). \
map(lambda d: "\t".join(str(x) for x in d)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock8/problem5/sol2')

DF3.rdd.map(tuple). \
map(lambda d: "\t".join(str(x) for x in d)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock8/problem5/sol3')