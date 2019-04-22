data_DF = sqlContext.sql('select * from snehaananthan_retail_db_txt.products_parquet')

DF1 = data_DF. \
filter('quantity >= 5000 and name like "pen%"'). \
select('productId','productCode','name','quantity','price','supplierId')

DF2 = data_DF. \
filter('quantity >= 5000 and name like "pen%" and price < 1.24'). \
select('productId','productCode','name','quantity','price','supplierId')

DF3 = data_DF. \
filter('quantity < 5000 and name not like "pen%"'). \
select('productId','productCode','name','quantity','price','supplierId')

DF4 = data_DF. \
filter("name in ('pen Red', 'pen Black')"). \
select('productId','productCode','name','quantity','price','supplierId')

DF5 = data_DF. \
filter('price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000'). \
select('productId','productCode','name','quantity','price','supplierId')


DF1.toJSON().coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock7/problem6/sol1', 
	compressionCodecClass='org.apache.hadoop.io.compress.SnappyCodec')

DF2.toJSON().coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock7/problem6/sol2', 
	compressionCodecClass='org.apache.hadoop.io.compress.SnappyCodec')

DF3.toJSON().coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock7/problem6/sol3', 
	compressionCodecClass='org.apache.hadoop.io.compress.SnappyCodec')

DF4.toJSON().coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock7/problem6/sol4', 
	compressionCodecClass='org.apache.hadoop.io.compress.SnappyCodec')

DF5.toJSON().coalesce(1). \
saveAsTextFile('/user/snehaananthan/mocks/mock7/problem6/sol5', 
	compressionCodecClass='org.apache.hadoop.io.compress.SnappyCodec')
