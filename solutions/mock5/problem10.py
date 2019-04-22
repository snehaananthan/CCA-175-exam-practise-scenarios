content = sc.textFile('/user/snehaananthan/mocks/data/Content.txt')

rdd = content. \
flatMap(lambda l: l.split('.')). \
map(lambda s: s.strip()). \
filter(lambda s: s!=''). \
map(lambda s: (s.split(' ')[0], s))

for i in rdd.collect(): print(i)

rdd.coalesce(1). \
saveAsSequenceFile('/user/snehaananthan/mocks/mock5/problem10', compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')