sqoop import-all-tables --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 6 \
--warehouse-dir '/user/snehaananthan/mocks/mock1/problem1/retail_db' \
--as-avrodatafile \
--outdir 'java'

sqoop import-all-tables --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 6 \
--hive-import \
--hive-database 'snehaananthan_retail_db_txt' \
--create-hive-table \
--fields-terminated-by '|' \
--outdir 'java'

sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
--connect 'jdbc:mysql://nn01.itversity.com:3306/nyse' \
--username=nyse_ro \
--password=itversity \
-m 8 \
--table 'stocks_eod' \
--target-dir '/user/snehaananthan/data/nyse_stocks' \
--compress \
--compression-codec 'snappy' \
--outdir 'java'