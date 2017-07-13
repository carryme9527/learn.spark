import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

schema = StructType([
  StructField('dt', TimestampType(), True),
  StructField('lat', DoubleType(), True),
  StructField('lon', DoubleType(), True),
  StructField('base', StringType(), True),
])

session = SparkSession.builder.appName('uber-travel').getOrCreate()
df = session.read.format('com.databricks.spark.csv').option('header', 'false').schema(schema).load('./data/uber.csv')

assembler = VectorAssembler().setInputCols(['lat', 'lon']).setOutputCol('features')
df = assembler.transform(df)

train, test = df.randomSplit([0.7, 0.3])
kmeans = KMeans().setK(8).setFeaturesCol('features').setPredictionCol('prediction')
model = kmeans.fit(train)
for center in model.clusterCenters():
  print center

pred = model.transform(test)
pred.show()

result = pred.select(hour(pred['dt']).alias('hour'), pred['prediction']) \
  .groupBy('hour', 'prediction').agg(count('prediction').alias('count')) \
  .orderBy(desc('count'))
result.show()

pred.groupBy('prediction').count().show()

model.write().overwrite().save('./savemodel')
