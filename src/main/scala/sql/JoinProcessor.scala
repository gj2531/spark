package sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

class JoinProcessor {
  private val spark: SparkSession = SparkSession.builder()
    .master("local[6]")
    .appName("agg processor")
    .getOrCreate()
  import spark.implicits._

  val person = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 3))
    .toDF("id", "name", "cityId")
  person.createOrReplaceTempView("person")

  val cities = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
    .toDF("id", "name")
  cities.createOrReplaceTempView("cities")

  @Test
  def introJoin(): Unit ={
    val person = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 0))
      .toDF("id", "name", "cityId")

    val cities = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
      .toDF("id", "name")

    val df: DataFrame = person.join(cities, person.col("cityId") === cities.col("id"))
      .select(person.col("id"), person.col("name"), cities.col("name").as("city"))
    df.createOrReplaceTempView("user_city")

    spark.sql("select id,name,city from user_city where city='Beijing'").show()
      //.show()
  }

  @Test
  def crossJoin(): Unit ={
    person.crossJoin(cities)
      .where(person.col("cityId") === cities.col("id"))
      //.show()

    spark.sql("select p.id,p.name,c.name from person p cross join cities c where p.cityId = c.id").show()
  }

  @Test
  def innerJoin(): Unit ={
    person.join(right = cities,joinExprs = person.col("cityId") === cities.col("id"),joinType = "inner")
      .show()

    spark.sql("select p.id,p.name,c.name from person p inner join cities c on p.cityid = c.id").show()
  }

  //全外连接
  @Test
  def fullOuter(): Unit ={
    person.join(right = cities,joinExprs = person.col("cityId") === cities.col("id"),joinType = "full")
      .show()

    spark.sql("select p.id,p.name,c.name from person p full outer join cities c on p.cityId = c.id").show()
  }

  @Test
  def leftAndRight(): Unit ={
    person.join(right = cities,joinExprs = person.col("cityId") === cities.col("id"),joinType = "left")
      .show()
    spark.sql("select p.id,p.name,c.name from person p left join cities c on p.cityId = c.id").show()


    person.join(right = cities,joinExprs = person.col("cityId") === cities.col("id"),joinType = "right")
      .show()
    spark.sql("select p.id,p.name,c.name from person p right join cities c on p.cityId = c.id").show()
  }


  //Anti只显示左侧未连接上右侧的数据
  //Semi只显示左侧连接上右侧的数据
  //Anti和Semi的共同点是 都不显示右侧的数据
  @Test
  def antiAndSemi(): Unit ={
    person.join(right = cities,joinExprs = person.col("cityId") === cities.col("id"),joinType = "left_Anti")
      //.show()

    person.join(right = cities,joinExprs = person.col("cityId") === cities.col("id"),joinType = "left_Semi")
      .show()
  }
}
