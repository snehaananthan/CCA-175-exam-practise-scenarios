vet = sc.textFile('/user/snehaananthan/mocks/data/veteran.txt')

from pyspark.sql import *
from pyspark.sql.functions import *

vet_DF = vet. \
map(lambda v:
	Row(badges=int(v.split(',')[7]) if v.split(',')[7] != '' else 0,
		age=int(v.split(',')[8]))
).toDF()

age_20_29_DF = vet_DF. \
filter('age between 20 and 29'). \
select(sum('badges').alias('badges')). \
withColumn('age_group', lit('20-29')). \
select('age_group', 'badges')



age_30_39_DF = vet_DF. \
filter('age between 30 and 39'). \
select(sum('badges').alias('badges')). \
withColumn('age_group', lit('30-39')). \
select('age_group', 'badges')

age_40_49_DF = vet_DF. \
filter('age between 40 and 49'). \
select(sum('badges').alias('badges')). \
withColumn('age_group', lit('40-49')). \
select('age_group', 'badges')

DF = age_20_29_DF.unionAll(age_30_39_DF).unionAll(age_40_49_DF).fillna(0)

sqlContext.setConf('spark.sql.parquet.compression.codec', 'snappy')
DF.coalesce(1).write.parquet('/user/snehaananthan/mocks/mock4/problem9')

