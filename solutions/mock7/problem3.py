create table snehaananthan_retail_db_txt.departments_sal(department_id int, department_name string, avg_salary int); 

insert into retail_export.departments_hive_snehaananthan_export select a.*, null from retail_db.departments a;

insert into departments_hive_snehaananthan_export values(777, "Not known",1000); 
insert into departments_hive_snehaananthan_export values(8888, null,1000); 
insert into departments_hive_snehaananthan_export values(666, null,1100); 

sqoop import --connect 'jdbc:mysql://ms.itversity.com:3306/retail_export' \
--username=retail_user \
--password=itversity \
-m 1 \
--table 'departments_hive_snehaananthan_export' \
--hive-import \
--hive-table 'snehaananthan_retail_db_txt.departments_sal' \
--null-string '' \
--null-non-string '-999' \
--outdir 'java'