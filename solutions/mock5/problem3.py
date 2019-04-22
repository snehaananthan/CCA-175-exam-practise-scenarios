sqoop import --connect 'jdbc:mysql://ms.itversity.com:3306/retail_export' \
--username=retail_user \
--password=itversity \
-m 1 \
--table 'departments_snehaananthan' \
--target-dir '/user/snehaananthan/mocks/mock5/problem3' \
--incremental append \
--check-column 'department_id' \
--last-value 7 \
--outdir 'java'


insert into departments_snehaananthan values(10, 'physics');
insert into departments_snehaananthan values(11, "Chemistry");
insert into departments_snehaananthan values(12, 'Maths');
insert into departments_snehaananthan values(13, 'Science');
insert into departments_snehaananthan values(14, 'Engineering');
