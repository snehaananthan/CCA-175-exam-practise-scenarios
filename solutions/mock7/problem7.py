data_DF = sqlContext.sql('select * from snehaananthan_retail_db_txt.products_parquet')

from pyspark.sql import *
from pyspark.sql.functions import *

DF1 = data_DF. \
filter('productCode is null'). \
select('productId','productCode','name','quantity','price','supplierId')

DF2 = data_DF. \
filter('name like "pen %"'). \
orderBy(desc('price')). \
select('productId','productCode','name','quantity','price','supplierId')

DF3 = data_DF. \
filter('name like "pen %"'). \
orderBy(desc('price'), 'quantity'). \
select('productId','productCode','name','quantity','price','supplierId')

DF4 = data_DF. \
orderBy(desc('price')). \
select('productId','productCode','name','quantity','price','supplierId'). \
limit(2)

DF1.rdd.map(tuple). \
map(lambda d: ",".join(str(x) for x in d)). \
saveAsTextFile('/user/snehaananthan/mocks/mock7/problem7/sol1', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')

DF2.rdd.map(tuple). \
map(lambda d: ",".join(str(x) for x in d)). \
saveAsTextFile('/user/snehaananthan/mocks/mock7/problem7/sol2', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')

DF3.rdd.map(tuple). \
map(lambda d: ",".join(str(x) for x in d)). \
saveAsTextFile('/user/snehaananthan/mocks/mock7/problem7/sol3', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')

DF4.rdd.map(tuple). \
map(lambda d: ",".join(str(x) for x in d)). \
saveAsTextFile('/user/snehaananthan/mocks/mock7/problem7/sol4', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')