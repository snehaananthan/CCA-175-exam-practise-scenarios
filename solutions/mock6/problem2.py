CREATE table departments_export_snehaananthan (department_id int(11), department_name varchar(45), created_date TIMESTAMP DEFAULT NOW());

sqoop export --connect 'jdbc:mysql://ms.itversity.com:3306/retail_export' \
--username=retail_user \
--password=itversity \
-m 1 \
--table 'departments_export_snehaananthan' \
--export-dir '/user/snehaananthan/mocks/mock6/problem1' \
--outdir 'java'