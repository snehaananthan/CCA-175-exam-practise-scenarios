data_DF = sqlContext.sql('select * from snehaananthan_retail_db_txt.products_parquet')

from pyspark.sql import *
from pyspark.sql.functions import *

DF1 = data_DF. \
agg(max('quantity').alias('max_quantity'),
	min('quantity').alias('min_quantity'),
	avg('quantity').alias('avg_quantity'),
	stddev('quantity').alias('standard_deviation_quantity'),
	sum('quantity').alias('total_quantity'))

DF2 = data_DF. \
groupBy('productCode'). \
agg(max('price').alias('max_price'),
	min('price').alias('min_price'))

DF3 = data_DF. \
groupBy('productCode'). \
agg(max('quantity').alias('max_quantity'),
	min('quantity').alias('min_quantity'),
	round(avg('quantity'), 2).alias('avg_quantity'),
	round(stddev('quantity'), 2).alias('standard_deviation_quantity'),
	sum('quantity').alias('total_quantity'))

DF4 = data_DF. \
groupBy('productCode'). \
agg(count('productId').alias('product_count'), 
	avg('price').alias('avg_price')). \
filter('product_count >= 3')

DF2.rdd.map(tuple). \
map(lambda d: " ".join(str(x) for x in d)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock7/problem9/sol2', compressionCodecClass='org.apache.hadoop.io.compress.SnappyCodec')

DF1.rdd.map(tuple). \
map(lambda d: " ".join(str(x) for x in d)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock7/problem9/sol1', compressionCodecClass='org.apache.hadoop.io.compress.SnappyCodec')

DF3.rdd.map(tuple). \
map(lambda d: " ".join(str(x) for x in d)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock7/problem9/sol3', compressionCodecClass='org.apache.hadoop.io.compress.SnappyCodec')

DF4.rdd.map(tuple). \
map(lambda d: " ".join(str(x) for x in d)). \
coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock7/problem9/sol4', compressionCodecClass='org.apache.hadoop.io.compress.SnappyCodec')