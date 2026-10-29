package solution

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.logging.log4j.core.config.Configurator
import org.apache.log4j.{Level, Logger}




object Main {


def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder
      .appName("hello-spark")
      .config(conf)
      .getOrCreate();
    
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("\n--- Spark Scala Hello World Program ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")
    println("Speed and Volume measurements files loading...")
    val speedDf = spark.read.parquet("/workspace/src/dataset/city_M_speed_5000_100.parquet")
    val volDf = spark.read.parquet("/workspace/src/dataset/city_M_volume_5000_100.parquet")
    println("Files loaded successfully.")
    println(s"Transforming data...")
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
    val unpivotedSpeedNotNullDf = unpivotedSpeedDf.filter($"speed_value".isNotNull)
    val unpivotedVolNotNullDf = unpivotedVolDf.filter($"volume_value".isNotNull)
    val volAndSpeedDf = unpivotedSpeedNotNullDf.join(unpivotedVolNotNullDf, Seq("timestamp", "node_id")).
                                    withColumn("node_id", $"node_id".cast("int"))
    println("Data transformation completed.")
    println("Loading static features and joining...")
    val staticDf = spark.read.parquet("/workspace/src/dataset/city_M_static_features.parquet").
                                              withColumn("node_id", monotonically_increasing_id())
    val allDataDf = volAndSpeedDf.join(staticDf, Seq("node_id"))
    println("Join completed.")
    println("Transformations for lag features and rolling averages...")
    val window = Window.partitionBy("node_id").orderBy("timestamp")
    val allDataWithLagDf = allDataDf.withColumn("speed_lag_1", lag("speed_value", 1).over(window))
                          .withColumn("speed_lag_2", lag("speed_value", 2).over(window))
                          .withColumn("volume_lag_1", lag("volume_value", 1).over(window))
                          .withColumn("volume_lag_2", lag("volume_value", 2).over(window))
    
    val lastHourSpec = Window
      .partitionBy("node_id")
      .orderBy("timestamp")
      .rowsBetween(-5, 0) 
    val allDataWithLagAndAvgDf = allDataWithLagDf
      .withColumn(
        "speed_last_30min_avg",
        when(count("*").over(lastHourSpec) < 5, lit(null))
          .otherwise(avg("speed_value").over(lastHourSpec))
      )
      .withColumn(
        "volume_last_30min_avg",
        when(count("*").over(lastHourSpec) < 5, lit(null)) 
          .otherwise(avg("volume_value").over(lastHourSpec))
      )
    val filteredDf = allDataWithLagAndAvgDf.filter($"speed_last_30min_avg".isNotNull && $"volume_last_30min_avg".isNotNull)
    val lastDf = filteredDf.
      withColumn("hour", hour(col("timestamp")) * 60 + minute(col("timestamp"))).
      drop("timestamp")
    val lastDfPersisted = lastDf.persist()

    val colsToVectorize = lastDfPersisted.columns.filter(c => 
      !c.startsWith("node_") && c != "speed_value" && c != "timestamp"
    )
    val ArrayCols = colsToVectorize.mkString(", ").split(", ").toArray
    println("Starting model training...")
    val assembler = new VectorAssembler()
      .setInputCols(ArrayCols)
      .setOutputCol("features")
    val output = assembler.transform(lastDfPersisted)
    val Array(training_data, test_data) = output.randomSplit(Array(0.7, 0.3))
    val speedRf = new RandomForestRegressor()
      .setLabelCol("speed_value")
      .setFeaturesCol("features")
      .setNumTrees(50)
    val speedModel = speedRf.fit(training_data)
    val speedPredictions = speedModel.transform(test_data).persist()
    val volRf = new RandomForestRegressor()
      .setLabelCol("volume_value")
      .setFeaturesCol("features")
      .setNumTrees(50)
    val volModel = volRf.fit(training_data)
    val volPredictions = volModel.transform(test_data).persist()
    println("Model training completed.")
    println("==================================================================")
    println("Evaluating Speed model performance...")
    val evaluator = new RegressionEvaluator()
      .setLabelCol("speed_value")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(speedPredictions)
    println(s"Speed Model : Root Mean Squared Error (RMSE) on test data = $rmse")
    println("Press Enter to finish...")
    System.in.read()

    println("==================================================================")
    println("Evaluating Volume model performance...")
    val volEvaluator = new RegressionEvaluator()
      .setLabelCol("volume_value")
      .setPredictionCol("prediction") 
      .setMetricName("rmse")
    val volRmse = volEvaluator.evaluate(volPredictions)
    println(s"Volume Model : Root Mean Squared Error (RMSE) on test data = $volRmse")
    println("Press Enter to finish...")
    System.in.read()


    println("Speed Predictions for next 30 minutes:")
    println("========")

    for (i <- 1 to 6) {
      val intervalPredictions = speedPredictions
          .select(format_number($"prediction", 2))  
          .collect()
          .map(_.getString(0))
          .mkString(",")

      println(s"$intervalPredictions,")
    }

    println("Volume Predictions for next 30 minutes:")
    println("========")

    for (i <- 1 to 6) {
      val intervalPredictions = volPredictions
          .select(format_number($"prediction", 2))  
          .collect()
          .map(_.getString(0))
          .mkString(",")

      println(s"$intervalPredictions,")
    }

    println("========")
    println("Saving models...")
    speedModel.write.overwrite().save("/workspace/src/models/speed_output.model")
    volModel.write.overwrite().save("/workspace/src/models/volume_output.model")
    println("Models saved to /workspace/src/models/")
    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
