orders_DF = sqlContext.sql('select order_id, order_date, order_customer_id from snehaananthan_retail_db_txt.orders')
orderItems_DF = sqlContext.sql('select order_item_order_id, order_item_subtotal from snehaananthan_retail_db_txt.order_items')

from pyspark.sql import *
from pyspark.sql.functions import *

join_DF = orders_DF.join(orderItems_DF, orders_DF.order_id == orderItems_DF.order_item_order_id). \
select('*', concat(substring('order_date', 1, 4), substring('order_date', 6, 2)).alias('month'))

DF = join_DF. \
groupBy('month', 'order_customer_id'). \
agg(sum('order_item_subtotal').alias('revenue')). \
orderBy(desc('revenue')). \
limit(5)

DF.registerTempTable('temp_table')

sqlContext.sql('create table snehaananthan_retail_db_txt.top5_customers_per_month as select * from temp_table')

