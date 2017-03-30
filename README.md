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
