create external table orders_sqoop (order_id int, 
order_date bigint,                    
order_customer_id int, 
order_status string) 
stored as avro 
location '/user/snehaananthan/warehouse/retail_stage.db/orders';

pyspark --master yarn \
--num-executors 4 \
--total-executor-cores 4 \
--executor-memory 512M \
--conf 'spark.ui.port=12999' 

from pyspark.sql import *
from pyspark.sql.functions import *

DF = sqlContext.sql('select * from snehaananthan_retail_db_txt.orders_sqoop')

DF1 = DF.select('order_id', to_date((DF.order_date/1000).cast('timestamp')).alias('order_date'), 'order_customer_id', 'order_status')

date_DF = DF1. \
groupBy('order_date'). \
agg(count('order_id').alias('total_orders')). \
orderBy(desc('total_orders'), 'order_date'). \
limit(1)

DF1.join(date_DF, 'order_date'). \
select('order_id', 'order_date', 'order_customer_id', 'order_status'). \
registerTempTable('temp_mock3_problem1')

sqlContext.sql('create table snehaananthan_retail_db_txt.mock3_problem1 stored as avro as select * from temp_mock3_problem1')






