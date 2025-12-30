package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Hello {

def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    // If no master is set (e.g., via spark-submit), default to local[*]
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder
      .appName("hello-spark")
      .config(conf)
      .getOrCreate();
    val sc = spark.sparkContext
    println("\n--- Spark Scala Hello World Program ---")
    println(s"Starting Spark job on ${spark.sparkContext.master}:")

    val data = sc.textFile("/workspace/data/dataset/wiki-logs-full/wiki-logs-full.txt")
    val filteredData = data.filter(line => line.contains(" ") && !line.startsWith("en") && line.trim.nonEmpty)
    val langAccessPairs = filteredData.map(line => {
        val columns = line.split(" ")
        (columns(0), columns(2).toInt)
    })
    
    val nlRDD = langAccessPairs.filter(columns => columns._1.startsWith("nl"))
    val accessByLang = langAccessPairs.reduceByKey(_ + _)
    val maxAccess = accessByLang.reduce((a,b) => if (a._2 > b._2) a else b)
    val totalNlAccess = nlRDD.reduce((a, b) => (a._1, a._2 + b._2))
    println(s"Le nombre total d'accès ${totalNlAccess}")
    println(s"Le nombre total d'accès ${maxAccess}")
    System.in.read()
    spark.stop()
    println("Spark Session stopped. Program finished.\n")
  }

}
