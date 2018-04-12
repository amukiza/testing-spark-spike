package org.sparktesting.transformations

import org.apache.spark.sql.Dataset
import org.sparktesting.domain.ViewsPerDay
import org.sparktesting.transformations.GroupByDayOfYear.spark

object SortByTotal {
  import spark.implicits._

  def apply(ds: Dataset[ViewsPerDay]): Dataset[ViewsPerDay] = {
    ds.sort($"total".desc).as[ViewsPerDay]
  }
}
