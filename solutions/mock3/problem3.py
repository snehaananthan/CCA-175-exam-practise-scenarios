sqoop import --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 1 \
--table 'categories' \
--where 'category_id between 1 and 22' \
--hive-import \
--hive-table 'snehaananthan_retail_db_txt.categories_subset' \
--create-hive-table \
--outdir 'java'