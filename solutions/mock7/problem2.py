sqoop import --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 1 \
--table 'departments' \
--hive-import \
--hive-table 'snehaananthan_retail_db_txt.departments_hive' \
--create-hive-table \
--outdir 'java'