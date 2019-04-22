create table products_hive 
(product_id int, 
product_category_id int, 
product_name string, 
product_description string, 
product_price float, 
product_image string, 
product_grade int,  
product_sentiment string);

sqoop job --delete 'job_mock4_prob2'

sqoop import --connect 'jdbc:mysql://ms.itversity.com:3306/retail_export' \
--username=retail_user \
--password=itversity \
-m 4 \
--table 'products_snehaananthan' \
--columns 'product_id,product_category_id,product_name,product_description,product_price,product_image' \
--where 'product_price != ""' \
--hive-import \
--hive-table 'snehaananthan_retail_db_txt.products_hive' \
--outdir 'java'