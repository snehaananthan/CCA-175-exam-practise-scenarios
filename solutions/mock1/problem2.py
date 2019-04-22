sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
--connect 'jdbc:mysql://nn01.itversity.com:3306/nyse' \
--username=nyse_ro \
--password=itversity \
-m 3 \
--table 'stocks_eod' \
--hive-import \
--hive-table 'snehaananthan_retail_db_txt.stocks_eod' \
--create-hive-table \
--outdir 'java' 

create table stocks_revenue_monthly as
select stockticker, substr(tradedate, 1, 7) trademonth,
sum(volume) monthly_volume
from stocks_eod
group by stockticker, substr(tradedate, 1, 7);

describe stocks_revenue_monthly;

create table nyse_export.stocks_eod_snehaananthan 
(stockticker varchar(30),                                      
trademonth varchar(30),                                       
monthly_volume bigint(20));

sqoop export --connect 'jdbc:mysql://nn01.itversity.com:3306/nyse_export' \
--username=nyse_ro \
--password=itversity \
-m 4 \
--table 'stocks_eod_snehaananthan' \
--export-dir '/apps/hive/warehouse/snehaananthan_retail_db_txt.db/stocks_revenue_monthly' \
--input-fields-terminated-by '\001' \
--outdir 'java'