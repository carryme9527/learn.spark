# learn.spark

try things when reading articles or websites.  
this is not a tutorial, and will not be.

## installation

[reference](https://github.com/aswergbh888/Spark)

## configuration

- jupyter notebook
    $SPARK_HOME/conf/spark-env.sh
    ``` shell
    PYSPARK_DRIVER_PYTHON=jupyter
    PYSPARK_DRIVER_PYTHON_OPTS="notebook --ip=* --NotebookApp.password='' --NotebookApp.token='' --no-browser"
    ```
- mongo
    $SPARK_HOME/conf/spark-defaults.sh
    ```
    spark.jars.packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0
    ```

## examples

- End to End Application for Monitoring Real-Time Uber Data Using Apache APIs: Kafka, Spark, HBase
    [Part 1: Spark Machine Learning](https://mapr.com/blog/monitoring-real-time-uber-data-using-spark-machine-learning-streaming-and-kafka-api-part-1/)
    [github](https://github.com/caroljmcdonald/spark-ml-kmeans-uber)
    [Part 2: Kafka and Spark Streaming](https://mapr.com/blog/monitoring-real-time-uber-data-using-spark-machine-learning-streaming-and-kafka-api-part-2/)
    [github](https://github.com/caroljmcdonald/mapr-sparkml-streaming-uber)
    [Part 3: Real-Time Dashboard Using Vert.x](https://mapr.com/blog/monitoring-uber-with-spark-streaming-kafka-and-vertx/)

    - my python code
        [Part 1](./examples/spark-ml-kmeans-uber/part1.py)
        [Part 2](./examples/spark-ml-kmeans-uber/part2.py)
