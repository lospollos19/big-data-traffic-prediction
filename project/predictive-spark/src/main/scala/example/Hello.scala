package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window


object Hello {

def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder
      .appName("hello-spark")
      .config(conf)
      .getOrCreate();
    import spark.implicits._

    println("\n--- Spark Scala Hello World Program ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val speedDf = spark.read.parquet("/workspace/project/dataset/city_M_speed_5000_100.parquet")
    val volDf = spark.read.parquet("/workspace/project/dataset/city_M_volume_5000_100.parquet")
    val nodeCols = speedDf.columns.filter(_.startsWith("node_"))
    
    val speedStackExpr = s"stack(${nodeCols.length}, ${nodeCols.map(c => {
      val id = c.replaceAll("[^0-9]", "")
      s"'$id', $c" 
    }).mkString(", ")}) as (node_id, speed_value)"

    val volStackExpr = s"stack(${nodeCols.length}, ${nodeCols.map(c => {
      val id = c.replaceAll("[^0-9]", "") 
      s"'$id', $c"
    }).mkString(", ")}) as (node_id, volume_value)"
    val unpivotedVolDf = volDf.select($"timestamp", expr(volStackExpr))
    val unpivotedSpeedDf = speedDf.select($"timestamp", expr(speedStackExpr))
    val longDf = unpivotedSpeedDf.join(unpivotedVolDf, Seq("timestamp", "node_id")).
                                    withColumn("node_id", $"node_id".cast("int"))
    val staticDf = spark.read.parquet("/workspace/project/dataset/city_M_static_features.parquet").
                                              withColumn("node_id", monotonically_increasing_id())
    

    val finalDf = longDf.join(staticDf, Seq("node_id"))

    val window = Window.partitionBy("node_id").orderBy("timestamp")
    val resultDf = finalDf.withColumn("speed_lag_1", lag("speed_value", 1).over(window))
                          .withColumn("speed_lag_2", lag("speed_value", 2).over(window))
                          .withColumn("volume_lag_1", lag("volume_value", 1).over(window))
                          .withColumn("volume_lag_2", lag("volume_value", 2).over(window))
    
    val lastHourSpec = Window
      .partitionBy("node_id")
      .orderBy("timestamp")
      .rowsBetween(-11, 0) 
    val resultDfWithAvg = resultDf
      .withColumn(
        "speed_last_hour_avg",
        when(count("*").over(lastHourSpec) < 12, lit(null))
          .otherwise(avg("speed_value").over(lastHourSpec))
      )
      .withColumn(
        "volume_last_hour_avg",
        when(count("*").over(lastHourSpec) < 12, lit(null)) 
          .otherwise(avg("volume_value").over(lastHourSpec))
      )

    resultDfWithAvg.select("timestamp", "node_id", "speed_value", "volume_value", 
                           "speed_last_hour_avg", "volume_last_hour_avg")
                    .orderBy("node_id", "timestamp")
                    .show(50, false)
          
    System.in.read()
    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
