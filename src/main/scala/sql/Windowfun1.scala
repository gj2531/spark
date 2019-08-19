package sql

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Windowfun1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[6]")
      .appName("udf")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val source: DataFrame = Seq(
      ("Thin", "Cell phone", 6000),
      ("Normal", "Tablet", 1500),
      ("Mini", "Tablet", 5500),
      ("ultra thin", "Cell phone", 5000),
      ("Very thin", "Cell phone", 6000),
      ("Big", "Tablet", 2500),
      ("Bendable", "Cell phone", 3000),
      ("Foldable", "Cell phone", 3000),
      ("Pro", "Tablet", 4500),
      ("Pro2", "Tablet", 6500)
    ).toDF("product", "category", "revenue")

    //定义窗口
    val window: WindowSpec = Window.partitionBy('category)
      .orderBy('revenue desc)

    //获得最贵的价格
    val maxPrice= max('revenue) over window

    source.select('product,'category,'revenue,(maxPrice - 'revenue) as "revenueDiffercence").show()
  }

}
