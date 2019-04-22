sqoop import --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 4 \
--table 'departments' \
--target-dir '/user/snehaananthan/mocks/mock5/problem1' \
--fields-terminated-by '|' \
--append \
--outdir 'java'