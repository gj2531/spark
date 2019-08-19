package sql

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

class Windowfun {
  @Test
  def windowFun(): Unit ={
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


    //定义窗口
    val window: WindowSpec = Window.partitionBy('category)
      .orderBy('revenue desc)
    //处理数据
    source.select('product,'category,dense_rank() over window as "rank")
      .where('rank <= 2)
      .show()

    source.createOrReplaceTempView("source")
    spark.sql("select product,category,revenue from " +
      "(select *,dense_rank() over (partition by category order by revenue desc) as rank from source) " +
      "where rank <= 2").show()
  }

}
