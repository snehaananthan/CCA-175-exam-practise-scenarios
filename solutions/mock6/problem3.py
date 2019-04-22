sqoop export --connect 'jdbc:mysql://ms.itversity.com:3306/retail_export' \
--username=retail_user \
--password=itversity \
-m 1 \
--table 'departments_snehaananthan' \
--export-dir '/user/snehaananthan/mocks/data/updated_departments.csv' \
--update-mode updateonly \
--update-key 'department_id' \
--outdir 'java'