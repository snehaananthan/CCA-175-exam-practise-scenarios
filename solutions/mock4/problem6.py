pyspark --master yarn \
--num-executors 2 \
--total-executor-cores 2 \
--executor-memory 512M \
--conf 'spark.ui.port=12999'

data = sc.textFile('/user/snehaananthan/mocks/data/data.csv')

rdd = data. \
map(lambda l: (l.split(',')[0], l.split(',')[1])). \
groupByKey(). \
map(lambda (k, v): (k, list(v))). \
zipWithIndex(). \
map(lambda ((k, v), id): (id+1, "\t".join(str(x) for x in v)))

rdd.coalesce(1).saveAsSequenceFile('/user/snehaananthan/mocks/mock4/problem6')