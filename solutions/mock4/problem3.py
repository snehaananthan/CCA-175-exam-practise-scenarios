create table products_external_snehaananthan  
(product_id int(11) primary Key,
product_category_id int(11),
product_name varchar(100),
product_description varchar(100),
product_price float,
product_image varchar(500),
product_grade int(11),
product_sentiment varchar(100));

sqoop job --delete 'job_mock4_prob3'

sqoop export --connect 'jdbc:mysql://ms.itversity.com:3306/retail_export' \
--username=retail_user \
--password=itversity \
-m 4 \
--table 'products_external_snehaananthan' \
--columns 'product_id,product_category_id,product_name,product_description,product_price,product_image' \
--export-dir '/apps/hive/warehouse/snehaananthan_retail_db_txt.db/products_hive' \
--input-fields-terminated-by '\001' \
--input-lines-terminated-by '\n' \
--outdir 'java'

