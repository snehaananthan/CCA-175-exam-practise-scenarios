sqoop import-all-tables --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 12 \
--hive-import \
--hive-database 'snehaananthan_retail_db_txt' \
--create-hive-table \
--outdir 'java'