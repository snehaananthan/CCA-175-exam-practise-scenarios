sqoop import-all-tables --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 12 \
--warehouse-dir '/user/snehaananthan/warehouse/retail_cca175.db' \
--as-avrodatafile \
--outdir 'java'

sqoop import --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 2 \
--table 'departments' \
--target-dir '/user/snehaananthan/mocks/mock2/problem2' \
--outdir 'java'