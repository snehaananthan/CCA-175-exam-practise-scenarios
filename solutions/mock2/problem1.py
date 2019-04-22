sqoop import --connect 'jdbc:mysql://ms.itversity.com:3306/retail_export' \
--username=retail_user \
--password=itversity \
-m 2 \
--table 'products_replica' \
--target-dir '/user/snehaananthan/mocks/mock2/problem1/products-text-part1' \
--where 'product_id <= 1111' \
--fields-terminated-by '*' \
--null-non-string '-1000' \
--null-string 'NA' \
--outdir 'java' \
--bindir 'java/compiled'

sqoop import --connect 'jdbc:mysql://ms.itversity.com:3306/retail_export' \
--username=retail_user \
--password=itversity \
-m 5 \
--table 'products_replica' \
--where 'product_id > 1111' \
--target-dir '/user/snehaananthan/mocks/mock2/problem1/products-text-part2' \
--null-non-string '-1000' \
--null-string 'NA' \
--fields-terminated-by '*' \
--outdir 'java' \
--bindir 'java/compiled'

sqoop merge \
--new-data '/user/snehaananthan/mocks/mock2/problem1/products-text-part2' \
--onto '/user/snehaananthan/mocks/mock2/problem1/products-text-part1' \
--target-dir '/user/snehaananthan/mocks/mock2/problem1/products-text-both-parts' \
--merge-key 'product_id' \
--class-name 'products_replica' \
--jar-file 'java/compiled/products_replica.jar'
