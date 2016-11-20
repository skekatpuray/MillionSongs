from pyspark import SparkConf, SparkContext
_conf = (SparkConf().setMaster("local").setAppName('intermediate cleaning'))
sc = SparkContext(conf = _conf)
lines = sc.textFile('MillionSongs/f2')
linesToSave = lines.map(lambda line: line[1:-1])
linesToSave.saveAsTextFile('MillionSongs/f3')
