package solution

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator

import java.util.Locale
import scala.collection.mutable.ArrayBuffer


object Main {

  // Measurements are 5 minutes apart: 6 steps = 30 minutes.
  val Horizon = 6
  val AvgWindow = 6

  // Wide format (timestamp, node_1, node_7, ...) to long format (timestamp, node_id, value).
  def unpivot(df: DataFrame, valueCol: String): DataFrame = {
    val nodeCols = df.columns.filter(_.startsWith("node_"))
    val stackExpr = s"stack(${nodeCols.length}, ${nodeCols.map(c => {
      val id = c.stripPrefix("node_")
      s"$id, cast(`$c` as double)"
    }).mkString(", ")}) as (node_id, $valueCol)"
    df.select(col("timestamp"), expr(stackExpr)).filter(col(valueCol).isNotNull)
  }

  def rmse(df: DataFrame, labelCol: String, predictionCol: String): Double =
    new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol(predictionCol)
      .setMetricName("rmse")
      .evaluate(df)

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: Main <speed.parquet> <volume.parquet> <static.parquet> [models-dir]")
      sys.exit(1)
    }
    val Array(speedPath, volumePath, staticPath) = args.take(3)
    val modelsDir = if (args.length > 3) args(3) else "models"

    val conf = new SparkConf()
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder()
      .appName("traffic-prediction")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println(s"Starting Spark job on ${spark.sparkContext.master}")
    print("Loading data...")
    val speedDf = unpivot(spark.read.parquet(speedPath), "speed_value")
    val volDf = unpivot(spark.read.parquet(volumePath), "volume_value")
    val volAndSpeedDf = speedDf.join(volDf, Seq("timestamp", "node_id"))

    // Row n (1-based) of the static file describes node_n.
    val staticRaw = spark.read.parquet(staticPath)
    val staticDf = spark.createDataFrame(
      staticRaw.rdd.zipWithIndex().map { case (row, i) => Row.fromSeq(row.toSeq :+ (i + 1).toInt) },
      staticRaw.schema.add("node_id", IntegerType))
    val allDataDf = volAndSpeedDf.join(staticDf, Seq("node_id")).persist()
    println(s"loaded ${allDataDf.select("node_id").distinct().count()} nodes.")

    print("Preparing training data...")
    // Only past values are used as features, so the targets never leak into them.
    val window = Window.partitionBy("node_id").orderBy("timestamp")
    val pastWindow = window.rowsBetween(-AvgWindow, -1)
    val featuresDf = allDataDf
      .withColumn("speed_lag_1", lag("speed_value", 1).over(window))
      .withColumn("speed_lag_2", lag("speed_value", 2).over(window))
      .withColumn("volume_lag_1", lag("volume_value", 1).over(window))
      .withColumn("volume_lag_2", lag("volume_value", 2).over(window))
      .withColumn(
        "speed_last_30min_avg",
        when(count("speed_value").over(pastWindow) < AvgWindow, lit(null))
          .otherwise(avg("speed_value").over(pastWindow))
      )
      .withColumn(
        "volume_last_30min_avg",
        when(count("volume_value").over(pastWindow) < AvgWindow, lit(null))
          .otherwise(avg("volume_value").over(pastWindow))
      )
      .withColumn("minute_of_day", hour(col("timestamp")) * 60 + minute(col("timestamp")))
      .filter($"speed_last_30min_avg".isNotNull && $"volume_last_30min_avg".isNotNull)

    val featureCols = staticRaw.columns ++ Array(
      "speed_lag_1", "speed_lag_2", "volume_lag_1", "volume_lag_2",
      "speed_last_30min_avg", "volume_last_30min_avg", "minute_of_day")
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
      .setHandleInvalid("skip")
    val output = assembler.transform(featuresDf).persist()

    // Time-based split: train on the first 70% of the timeline, test on the rest.
    val steps = output.select("timestamp").distinct()
    val cutoff = steps.orderBy("timestamp").offset((steps.count() * 0.7).toInt).first().get(0)
    val trainingData = output.filter(col("timestamp") < lit(cutoff))
    val testData = output.filter(col("timestamp") >= lit(cutoff))
    println("done.")

    print("Training model...")
    val speedModel = new RandomForestRegressor()
      .setLabelCol("speed_value")
      .setFeaturesCol("features")
      .setNumTrees(50)
      .fit(trainingData)
    val volModel = new RandomForestRegressor()
      .setLabelCol("volume_value")
      .setFeaturesCol("features")
      .setNumTrees(50)
      .fit(trainingData)
    println("done.")

    val speedPredictions = speedModel.transform(testData).persist()
    val volPredictions = volModel.transform(testData).persist()
    val fmt = (x: Double) => "%.2f".formatLocal(Locale.ROOT, x)
    println(s"Speed RMSE on test data:  ${fmt(rmse(speedPredictions, "speed_value", "prediction"))} " +
      s"(last value baseline: ${fmt(rmse(testData, "speed_value", "speed_lag_1"))})")
    println(s"Volume RMSE on test data: ${fmt(rmse(volPredictions, "volume_value", "prediction"))} " +
      s"(last value baseline: ${fmt(rmse(testData, "volume_value", "volume_lag_1"))})")

    print("Saving model...")
    speedModel.write.overwrite().save(s"$modelsDir/speed_output.model")
    volModel.write.overwrite().save(s"$modelsDir/volume_output.model")
    println(s"done ($modelsDir/).")

    // Recursive forecast: each predicted step becomes the most recent history row.
    // k = 1 is the latest step, k = AvgWindow the oldest one we keep.
    val recent = Window.partitionBy("node_id").orderBy(col("timestamp").desc)
    var history = allDataDf
      .withColumn("k", row_number().over(recent))
      .filter(col("k") <= AvgWindow)
      .select("node_id", "k", "speed_value", "volume_value")
      .localCheckpoint()
    val lastMinute = allDataDf.agg(max("timestamp").as("t"))
      .select(hour(col("t")) * 60 + minute(col("t")))
      .first().getInt(0)

    val forecasts = ArrayBuffer[Array[String]]()
    for (h <- 1 to Horizon) {
      val stepFeatures = history.groupBy("node_id").agg(
          max(when(col("k") === 1, col("speed_value"))).as("speed_lag_1"),
          max(when(col("k") === 2, col("speed_value"))).as("speed_lag_2"),
          max(when(col("k") === 1, col("volume_value"))).as("volume_lag_1"),
          max(when(col("k") === 2, col("volume_value"))).as("volume_lag_2"),
          avg("speed_value").as("speed_last_30min_avg"),
          avg("volume_value").as("volume_last_30min_avg"),
          count("*").as("n"))
        .filter(col("n") === AvgWindow)
        .withColumn("minute_of_day", lit((lastMinute + 5 * h) % (24 * 60)))
        .join(staticDf, Seq("node_id"))
      val withSpeed = speedModel.transform(assembler.transform(stepFeatures))
        .withColumnRenamed("prediction", "speed_value")
      val step = volModel.transform(withSpeed)
        .withColumnRenamed("prediction", "volume_value")
        .select("node_id", "speed_value", "volume_value")
        .localCheckpoint()

      forecasts += step.orderBy("node_id")
        .select(format_string("%.2f", col("speed_value")))
        .collect()
        .map(_.getString(0))
      history = step.withColumn("k", lit(1))
        .unionByName(history.withColumn("k", col("k") + 1).filter(col("k") <= AvgWindow))
        .localCheckpoint()
    }

    spark.stop()

    println("Predictions for next 30 minutes:")
    println("========")
    forecasts.foreach(line => println(line.mkString(",")))
  }

}
