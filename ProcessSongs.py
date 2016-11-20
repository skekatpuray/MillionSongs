from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import math
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModelBase
import numpy as np
from pyspark.sql import Row
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.types import StringType,StructField,StructType,IntegerType,NumericType,FloatType
from pyspark.mllib.feature import StandardScaler



EXTRACT_LOCATION = 'MillionSongs/f3'

list1 = []

_conf = (SparkConf().setMaster("local").setAppName("Million Songs"))
sc = SparkContext(conf = _conf)
sqlContext = SQLContext(sc)

lineItems = (sc.textFile(EXTRACT_LOCATION)
            .map(lambda line: line.split(',')))

songsRdd = lineItems.map(lambda item: ( float(item[0]), 
                                        int(item[1] or 0) 
                                        ,int(item[2] or 0)
                                        ,float(item[3] or 0.00)
                                        ,(0.00 if item[4] == ' None' else float(item[4]))
                                        ,(0 if item[5] == ' None' else int(item[5]))
                                        ,(0.00 if item[6] == ' None' else float(item[6]))
                                        ,(0.00 if item[7] == ' None' else float(item[7]))
                                        ,(0.00 if item[8] == ' None' else float(item[8]))
                                        ,(0.00 if item[9] == ' None' else float(item[9]))
                                        ,(0.00 if item[10] == ' None' else float(item[10]))
                                        ,(0 if item[11] == ' None' else int(item[11]))
                                        ,(0.00 if item[12] == ' None' else float(item[12]))
                                        ))


songSchema = StructType([StructField("artist_hotttnesss", FloatType(), False),
                         StructField("artist_playmeid", IntegerType(), False),
                         StructField("analysis_sample_rate", IntegerType(), False),
                         StructField("duration", FloatType(), False),
                         StructField("end_of_fade_in", FloatType(), False),
                         StructField("key", IntegerType(), False),
                         StructField("key_confidence", FloatType(), False),
                         StructField("loudness", FloatType(), False),
                         StructField("mode_confidence", FloatType(), False),
                         StructField("start_of_fade_out", FloatType(), False),
                         StructField("tempo", FloatType(), False),
                         StructField("time_signature", IntegerType(), False),
                         StructField("time_signature_confidence", FloatType(), False)])


def getLabelFeatures(point):
    label = float(point[0])
    features = point[1:]
    return LabeledPoint(label,features)


training, test = songsRdd.randomSplit([0.8, 0.2])

trainParsedData = training.map(getLabelFeatures)

model = LinearRegressionWithSGD.train(trainParsedData, iterations=100, step=0.00000001)

# Evaluate the model on training data
#Following code referred from https://spark.apache.org/docs/latest/mllib-linear-methods.html#regression
trainValuesAndPreds = trainParsedData.map(lambda p: (p.label, model.predict(p.features)))
MSE = valuesAndPreds \
    .map(lambda (v, p): (v - p)**2) \
    .reduce(lambda x, y: x + y) / trainValuesAndPreds.count()
print("Mean Squared Error = " + str(MSE))

#Apply on test
testParsedData = test.map(getLabelFeatures)
testValuesAndPreds = testParsedData.map(lambda p: (p.label, model.predict(p.features)))





