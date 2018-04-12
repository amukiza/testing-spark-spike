package org.sparktesting.example

import java.sql.Timestamp

import org.apache.spark.sql.Dataset
import org.scalatest.{Matchers, WordSpec}
import org.sparktesting.SparkSessionWrapper
import org.sparktesting.domain.{PageView, ViewsPerDay}
import org.sparktesting.transformations.{GroupByDayOfYear, SortByTotal}

class SortByTotalSpec extends WordSpec
  with SparkSessionWrapper with Matchers {
  import spark.implicits._

  private val data = Seq(
    PageView(Timestamp.valueOf("2015-03-16 00:19:39"), "desktop", 24),
    PageView(Timestamp.valueOf("2015-03-16 00:10:39"), "mobile", 15),
    PageView(Timestamp.valueOf("2015-03-16 00:38:11"), "desktop", 25),
    PageView(Timestamp.valueOf("2015-03-16 00:42:40"), "mobile", 22),
    PageView(Timestamp.valueOf("2015-03-17 00:52:24"), "desktop", 20),
    PageView(Timestamp.valueOf("2015-03-17 00:54:16"), "mobile", 16),
    PageView(Timestamp.valueOf("2015-03-18 00:54:16"), "desktop", 20),
    PageView(Timestamp.valueOf("2015-03-18 00:54:16"), "mobile", 17)
  )

  val pageViewDF: Dataset[PageView] = spark.createDataset(data).as[PageView]
  "SortByTotalPageViews" should {

    "Sort rdd by total page views" in {
      val actual = GroupByDayOfYear(pageViewDF) // We can pass in a data-set in the expected state (I cheated)
                .transform[ViewsPerDay](SortByTotal(_))

      actual.first().day_of_year should be(75)
      actual.first().total should be(86)
    }
  }
}
