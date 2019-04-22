sqoop job --delete job_mock3_prob2

sqoop job --create job_mock3_prob2 \
-- import --connect 'jdbc:mysql://ms.itversity.com:3306/retail_export' \
--username=retail_user \
--password=itversity \
-m 4 \
--table 'products_snehaananthan' \
--target-dir '/user/snehaananthan/mocks/mock3/problem2/products-incremental' \
--incremental append \
--check-column 'product_id' \
--last-value 0

sqoop job --exec job_mock3_prob2