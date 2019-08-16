package sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.Test

class ReadWrite {
  val spark: SparkSession = SparkSession.builder()
    .master("local[6]")
    .appName("reader1")
    .getOrCreate()

  @Test
  def reader1(): Unit ={
    val spark: SparkSession = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()
    spark.read
      .format("csv")
      .option("header",value = true)
      .option("inferschema",value = true)
      .load("dataset/BeijingPM20100101_20151231.csv")
      .show(10)

    spark.read
      .option("header",value = true)
      .option("inferschema",value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

  }

  @Test
  def writer(): Unit ={
    val spark: SparkSession = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()
    val df: DataFrame = spark.read.option("header",value = true).csv("dataset/BeijingPM20100101_20151231.csv")
    df.write.json("dataset/beijing_pm.json")

    df.write.format("json").save("dataset/beijing_pm2.json")
  }

  @Test
  def parquet(): Unit ={
    val df: DataFrame = spark.read
      .option("header", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

    //把数据写为parquet格式
    //如果不指定数据写入格式 那么默认使用的就是parquet格式
    //数据写入模式 四种 追加 覆盖 报错 忽略
    df.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("dataset/beijing_pm3.parquet")
  }

  @Test
  def json(): Unit ={
    val df: DataFrame = spark.read
      .option("header", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

    df.write
      .json("dataset/beijing_pm4.json")

    spark.read
      .json("dataset/beijing_pm4.json")
      .show()
  }
}
