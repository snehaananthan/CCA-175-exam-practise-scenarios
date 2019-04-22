sqoop import --connect 'jdbc:mysql://ms.itversity.com:3306/retail_export' \
--username=retail_user \
--password=itversity \
-m 1 \
--table 'departments_ts_snehaananthan' \
--target-dir '/user/snehaananthan/mocks/mock6/problem1' \
--incremental append \
--check-column 'created_date' \
--last-value ' 2018-05-07 21:05:47.0' \
--outdir 'java'

insert into departments_ts_snehaananthan (department_id, department_name) values (110, "Civil");
insert into departments_ts_snehaananthan (department_id, department_name) values (111, "Mechanical");
insert into departments_ts_snehaananthan (department_id, department_name) values (112, "Automobile");
insert into departments_ts_snehaananthan (department_id, department_name) values (113, "Pharma");
insert into departments_ts_snehaananthan (department_id, department_name) values (114, "social engineering");