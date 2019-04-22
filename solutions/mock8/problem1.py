sqoop export --connect 'jdbc:mysql://ms.itversity.com:3306/retail_export' \
--username=retail_user \
--password=itversity \
-m 1 \
--table 'departments_hive_snehaananthan_export' \
--columns 'id,department_name' \
--export-dir '/apps/hive/warehouse/snehaananthan_retail_db_txt.db/departments_hive' \
--input-null-string '' \
--input-null-non-string '-999' \
--input-fields-terminated-by '\001' \
--outdir 'java'

insert into departments_hive values (100, '');
insert into departments_hive values (101, '');
insert into departments_hive values (-999, 'bongu');