import java.nio.file.Paths

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.sparktesting.SparkSessionWrapper
import org.sparktesting.domain.PageView
import org.sparktesting.transformations.{GroupByDayOfYear, SortByTotal}

object Main extends App with SparkSessionWrapper {

  import spark.implicits._

  val resource = this.getClass.getClassLoader.getResource("data/pageviews-by-second-tsv")
  val dataSource = Paths.get(resource.toURI).toString
  val schema = ScalaReflection.schemaFor[PageView].dataType.asInstanceOf[StructType]

  val pageViews = spark.read
    .option("delimiter", "\t")
    .option("header", "true")
    .schema(schema)
    .csv(dataSource)
    .as[PageView]

  private val ds = GroupByDayOfYear(pageViews)
    .transform(SortByTotal(_))

    //.transform(SomeOtherTransformation(_))
    //.transform(..)
    //.transform(..)

  ds.explain(true)

  ds.show()
}
