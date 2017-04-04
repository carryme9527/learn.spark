# -*- coding: UTF-8 -*-

import json
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

input_uri = ''
output_uri = ''

def get_spark():
  conf = SparkConf()
  sc = SparkContext(conf=conf)
  spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .getOrCreate()
  return sc, spark

# issue regarding Mixed Types
# http://mongodb.2344371.n4.nabble.com/Mongodb-Spark-Conflict-datatype-issue-td11263.html

# workaround
# use pipeline to select only what you want
# https://docs.mongodb.com/manual/reference/operator/aggregation/group/

if __name__ == '__main__':
  sc, spark = get_spark()
  pipeline = [
    {
      '$match': {
        'column': 'value',
      },
    },
    {
      '$group': {
        '_id': '$column',
        'new_column_01': {
          '$push': '$column',
        },
        'new_column_02': {
          '$sum': 1,
        },
      } 
    },            
  ]               
                  
  df = spark.read \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option('pipeline',json.dumps(pipeline)) \
    .load()
    
  results = df.rdd \
    .reduceByKey(lambda x,y: x+y) \
    .collect()

