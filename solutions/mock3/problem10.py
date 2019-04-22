files = sc.textFile('/user/snehaananthan/mocks/data/files')  
remove_list = ['at', 'imperdiet', 'dui', 'accumsan', 'sit', 'amet', 'nulla', 'facilisi', 'morbi', 'tempus', 'iaculis', 'urna', 'id', 'volutpat', 'lacus', 'laoreet', 'non']

remove = sc.parallelize(remove_list)

from operator import add

rdd1 = files. \
flatMap(lambda l: l.split(' ')). \
map(lambda w: (w.replace(',', '').replace('.', ''), 1)). \
reduceByKey(add)

rdd2 = remove. \
map(lambda w: (w, 1))

rdd = rdd1.leftOuterJoin(rdd2). \
filter(lambda (w, (c1, c2)): c2 == None). \
map(lambda (w, (c1, c2)): (c1, w)). \
sortByKey(False). \
map(lambda (c,w): w + "," + str(c))

for i in rdd.take(10): print(i)

rdd.coalesce(1).saveAsTextFile('/user/snehaananthan/mocks/mock3/problem10', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')



