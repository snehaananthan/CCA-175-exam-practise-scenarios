pyspark --master yarn \
--num-executors 4 \
--total-executor-cores 4 \
--executor-memory 512M \
--conf 'spark.ui.port=12999'

f1 = sc.textFile('/user/snehaananthan/mocks/data/wordfile1.txt')
f2 = sc.textFile('/user/snehaananthan/mocks/data/wordfile2.txt')
f3 = sc.textFile('/user/snehaananthan/mocks/data/wordfile3.txt')
f4 = sc.textFile('/user/snehaananthan/mocks/data/wordfile4.txt')

from operator import add
from pyspark.sql import *
from pyspark.sql.functions import *

f1_DF = f1. \
flatMap(lambda l: l.split(" ")). \
map(lambda w: (w.replace(',','').replace('.',''), 1)). \
reduceByKey(add). \
map(lambda (w, c): Row(word=w, count=int(c))). \
toDF()

f2_DF = f2. \
flatMap(lambda l: l.split(" ")). \
map(lambda w: (w.replace(',','').replace('.',''), 1)). \
reduceByKey(add). \
map(lambda (w, c): Row(word=w, count=int(c))). \
toDF()

f3_DF = f3. \
flatMap(lambda l: l.split(" ")). \
map(lambda w: (w.replace(',','').replace('.',''), 1)). \
reduceByKey(add). \
map(lambda (w, c): Row(word=w, count=int(c))). \
toDF()

f4_DF = f4. \
flatMap(lambda l: l.split(" ")). \
map(lambda w: (w.replace(',','').replace('.',''), 1)). \
reduceByKey(add). \
map(lambda (w, c): Row(word=w, count=int(c))). \
toDF()

DF1 = f1_DF. \
select('*', dense_rank().over(Window.orderBy(desc('count'))).alias('rank')). \
filter('rank = 1'). \
withColumn('filename', lit('file1')). \
select('filename', 'word')

DF2 = f2_DF. \
select('*', dense_rank().over(Window.orderBy(desc('count'))).alias('rank')). \
filter('rank = 1'). \
withColumn('filename', lit('file2')). \
select('filename', 'word')

DF3 = f3_DF. \
select('*', dense_rank().over(Window.orderBy(desc('count'))).alias('rank')). \
filter('rank = 1'). \
withColumn('filename', lit('file3')). \
select('filename', 'word')

DF4 = f4_DF. \
select('*', dense_rank().over(Window.orderBy(desc('count'))).alias('rank')). \
filter('rank = 1'). \
withColumn('filename', lit('file4')). \
select('filename', 'word')

DF = DF1.unionAll(DF2).unionAll(DF3).unionAll(DF4)

rdd = DF.groupBy('filename'). \
agg(collect_list('word')). \
rdd.map(tuple). \
map(lambda (k, v): k + "\t" + "\t".join(str(x) for x in v))

for i in rdd.collect(): print(i)

rdd.coalesce(1).saveAsTextFile('/user/snehaananthan/mocks/mock5/problem4')