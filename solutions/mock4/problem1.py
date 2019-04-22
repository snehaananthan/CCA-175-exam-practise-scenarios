sqoop import --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 2 \
--table departments \
--where 'department_id between 1 and 25' \
--columns 'department_id,department_name' \
--target-dir '/user/snehaananthan/mocks/mock4/problem1' \
--outdir 'java' 
