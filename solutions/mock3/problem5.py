content = sc.textFile('/user/snehaananthan/mocks/data/Content.txt')
remove = sc.textFile('/user/snehaananthan/mocks/data/Remove.txt')

from operator import add
from pyspark.sql import *

content_rdd = content. \
flatMap(lambda l: l.split(' ')). \
filter(lambda w: w != ''). \
map(lambda w: (w.replace(',', '').replace('.', ''), 1)). \
reduceByKey(add)

remove_rdd = remove. \
flatMap(lambda l: l.split(',')). \
map(lambda w: (w, 1))

rdd = content_rdd.leftOuterJoin(remove_rdd). \
filter(lambda (w, (c1, c2)): c2 == None). \
map(lambda (w, (c1, c2)): w + " " + str(c1))

rdd.coalesce(1).saveAsTextFile('/user/snehaananthan/mocks/mock3/problem5')

