data_DF = sqlContext.sql('select * from snehaananthan_retail_db_txt.products_parquet')

from pyspark.sql import *
from pyspark.sql.functions import *

DF1 = data_DF. \
select(concat_ws(' - ', 'productCode','name').alias('product_description'))

DF2 = data_DF. \
select('price').distinct()

DF3 = data_DF. \
select('name', 'price').distinct()

DF4 = data_DF. \
select('productId','productCode','name','quantity','price','supplierId'). \
orderBy('productCode', desc('productId'))

DF5 = data_DF. \
agg(count('productId').alias('num_of_products'))

DF6 = data_DF. \
groupBy('productCode'). \
agg(count('productId').alias('num_of_products'))

DF1.write.orc('/user/snehaananthan/mocks/mock7/problem8/sol1')
DF2.write.orc('/user/snehaananthan/mocks/mock7/problem8/sol2')
DF3.write.orc('/user/snehaananthan/mocks/mock7/problem8/sol3')
DF4.write.orc('/user/snehaananthan/mocks/mock7/problem8/sol4')
DF5.write.orc('/user/snehaananthan/mocks/mock7/problem8/sol5')
DF6.write.orc('/user/snehaananthan/mocks/mock7/problem8/sol6')