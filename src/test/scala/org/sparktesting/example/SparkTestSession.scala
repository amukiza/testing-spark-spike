package org.sparktesting.example

import org.apache.spark.sql.SparkSession

trait SparkTestSession {
  // We can implement test closures that start and stop the spark context in here

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }
}
