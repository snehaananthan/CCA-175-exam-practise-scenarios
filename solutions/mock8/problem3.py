sqoop import --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 1 \
--table 'categories' \
--target-dir '/user/snehaananthan/mocks/mock8/problem3/categories_target_job' \
--outdir 'java'