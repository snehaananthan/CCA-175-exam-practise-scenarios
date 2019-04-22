sqoop import --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 1 \
--table 'departments' \
--target-dir '/user/snehaananthan/mocks/mock8/problem2/departments_text' \
--compress \
--compression-codec 'gzip' \
--outdir 'java'

sqoop import --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 1 \
--table 'departments' \
--target-dir '/user/snehaananthan/mocks/mock8/problem2/departments_sequence' \
--as-sequencefile \
--compress \
--compression-codec 'gzip' \
--outdir 'java'

sqoop import --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 1 \
--table 'departments' \
--target-dir '/user/snehaananthan/mocks/mock8/problem2/departments_avro' \
--as-avrodatafile \
--compress \
--compression-codec 'snappy' \
--outdir 'java'

sqoop import --connect 'jdbc:mysql://nn01.itversity.com:3306/retail_db' \
--username=retail_dba \
--password=itversity \
-m 1 \
--table 'departments' \
--target-dir '/user/snehaananthan/mocks/mock8/problem2/departments_parquet' \
--as-parquetfile \
--compress \
--compression-codec 'snappy' \
--outdir 'java'