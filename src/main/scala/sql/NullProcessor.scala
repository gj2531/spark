package sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.Test

class NullProcessor {

  val spark: SparkSession = SparkSession.builder()
    .master("local[6]")
    .appName("null processor")
    .getOrCreate()
  @Test
  def nullAndNan(): Unit ={



    //这种方式会把NAN的值读取为String类型
//    spark.read
//      .option("header",value = true)
//      .option("inferSchema",value = true)
//      .csv("dataset/beijingpm_with_nan.csv")

    val schema = StructType(
      List(
        StructField("id", LongType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

    val sourceDF= spark.read
      .option("header", value = true)
      .schema(schema)
      .csv("dataset/beijingpm_with_nan.csv")

    //sourceDF.show()
    //todo 当某一行的所有列都是NaN或Null时丢弃此列
    //sourceDF.na.drop("all").show()
    //todo 当某一行的特定列的所有值都是NaN或Null时丢弃此列 如下：如果此行的pm和id这两列都是缺失值时丢弃次列
    //sourceDF.na.drop("all",List("pm","id")).show()
    //todo 当某一行的所有列有任意一列的值为NaN或Null时丢弃此列
    //sourceDF.na.drop("any").show()
    //todo 当某一行的特定列有任意值为NaN或Null时丢弃此列
    //sourceDF.na.drop("any",List("id","pm")).show()
    //todo 填充所有包含 null 和 NaN 的列
    //sourceDF.na.fill(0).show()
    //todo 填充特定包含 null 和 NaN 的列
    //sourceDF.na.fill(2,List("pm")).show()

  }

  //针对字符串类型的缺失值进行处理
  @Test
  def strProcessor(): Unit ={
    //读取数据
    val sourceDF = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

    //sourceDF.show()
    import spark.implicits._
    //todo 丢弃
    //sourceDF.where('PM_Dongsi =!= "NA").show()
    //todo 替换
//    sourceDF.select(
//      'No as "id",'year,'month,'day,'hour,'season,
//      when('PM_Dongsi === "NA",Double.NaN)
//        .otherwise('PM_Dongsi cast DoubleType)
//        .as("PM")
//    ).show()

    //todo 这种转换方法 必须保证原数据类型和转换后的数据类型一致 这种方法不是很方便
    //sourceDF.na.replace("PM_Dongsi",Map("NA" -> "NaN","NULL" -> "null")).show()
  }
}
