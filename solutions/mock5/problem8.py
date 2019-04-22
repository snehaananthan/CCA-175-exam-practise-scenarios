rdd = sc.parallelize(["dog", "tiger", "lion", "cat", "panther", "eagle"])


res = rdd. \
map(lambda w: (w, len(w)))

res. \
toDF(schema=['string', 'length']). \
coalesce(1). \
write.orc('/user/snehaananthan/mocks/mock5/problem8')