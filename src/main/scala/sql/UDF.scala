package sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

class UDF {

  @Test
  def udf1(): Unit ={
    val spark: SparkSession = SparkSession.builder()
      .master("local[6]")
      .appName("udf")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

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

    //需求1：每个类别的总价
    //  source.groupBy('category)
    //    .agg(sum('revenue))
    //    .show()

    //需求2：名称变为小写
    source.select(lower('product))
      .show()

    //需求3：把价格变为字符串 6000 -> 6K
    var toStrudf = udf(toStr _)
    source.select('product,'category,toStrudf('revenue))
      .show()
  }

  def toStr(revenue: Long): String ={
    (revenue / 1000 ) + "K"
  }
}
