import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from collections import namedtuple
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeansModel


sc = SparkContext(conf=SparkConf())
session = SparkSession.builder.appName('uber-travel').getOrCreate()

Uber = namedtuple('Uber', ['dt', 'lat', 'lon', 'base'])
def parseUber(string):
  p = string.split(',')
  return Uber(p[0], float(p[1]), float(p[2]), p[3])

model = KMeansModel.load('./savemodel')
for center in model.clusterCenters():
  print center

# skip Kafka setting things

rdd = sc.textFile('./data/uber.csv')
if (not rdd.isEmpty()):
  count = rdd.count()
  print 'count received ', count
  df = rdd.map(parseUber).toDF()
  df.show()

  assembler = VectorAssembler().setInputCols(['lat', 'lon']).setOutputCol('features')
  df = assembler.transform(df)

  pred = model.transform(df)
  pred.show()
  pred.createOrReplaceTempView('uber')
  res = session.sql('select dt, lat, lon, base, prediction as cluster from uber')
  res.show()
