package org.sparktesting.transformations

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.dayofyear
import org.sparktesting.SparkSessionWrapper
import org.sparktesting.domain.{PageView, ViewsPerDay}

object GroupByDayOfYear extends SparkSessionWrapper {

  import spark.implicits._

  def apply(pageViews: Dataset[PageView]): Dataset[ViewsPerDay] = {
    pageViews.withColumn("day_of_year", dayofyear($"timestamp"))
      .groupBy($"day_of_year")
      .sum("requests")
      .withColumnRenamed("sum(requests)", "total")
      .as[ViewsPerDay]
  }
}
