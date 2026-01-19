# Hello Spark

## Commands
These are some (groups of) commands useful for setting up, understanding, experimenting, testing, ...

```
cd hello-spark

/opt/spark/bin/spark-submit --class org.apache.spark.examples.SparkPi --master local[8] /opt/spark/examples/jars/spark-examples_2.13-4.0.0.jar 100
/opt/spark/bin/spark-submit --class org.apache.spark.examples.SparkPi --master spark://spark-master:7077 /opt/spark/examples/jars/spark-examples_2.13-4.0.0.jar 100

cd hello-spark

sbt run

sbt clean assembly
spark-submit --class example.Hello --master local[*] target/scala-2.13/myProject.jar
spark-submit --class example.Hello --master spark://spark-master:7077 target/scala-2.13/myProject.jar
```