package sql

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.junit.Test

class AggProcessor {
  //创建saprkSession
  private val spark: SparkSession = SparkSession.builder()
    .master("local[6]")
    .appName("agg processor")
    .getOrCreate()
  import spark.implicits._

  import org.apache.spark.sql.functions._
  @Test
  def groupby(): Unit ={
    //读取数据
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

    val sourceDF: DataFrame = spark.read
      .schema(schema)
      .option("header", value = true)
      .csv("dataset/beijingpm_with_nan.csv")

    //数据清洗
    val sourceClean: Dataset[Row] = sourceDF.where('pm =!= Double.NaN)
    val groupedDataset: RelationalGroupedDataset = sourceClean.groupBy('year , 'month )
    //好像不太行 换个方式
    //groupedDataset.avg("pm").as("avg_pm").orderBy("avg_pm").show()
    groupedDataset.agg(avg('pm) as "pm_avg")
      .orderBy('pm_avg desc)
      .show()

    //还有一种方式
    groupedDataset.avg("pm").orderBy("avg(pm)").show()
    //或者
    groupedDataset.avg("pm")
      .select($"avg(pm)" as "pm_avg")
      .orderBy("pm_avg")
      .show()
  }

  @Test
  def multiAgg(): Unit ={
     val schemaFinal = StructType(
      List(
        StructField("source", StringType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

     val pmFinal = spark.read
      .schema(schemaFinal)
      .option("header", value = true)
      .csv("dataset/pm_final.csv")

    //需求1：不同年 不同来源的PM值的平均值
    val postAndyearDF: DataFrame = pmFinal.groupBy('source, 'year)
      .agg(avg('pm) as "pm")
    //需求2：在整个数据集当中 按照不同的来源来统计PM值的平均值
    val postDF: DataFrame = pmFinal.groupBy('source)
      .agg(avg('pm) as "pm")
      .select('source, lit(null) as "year", 'pm)
    //合并到同一个结果集当中
    postAndyearDF.union(postDF)
      .sort('source,'year asc_nulls_last,'pm)
      .show()
  }

  @Test
  def rollup(): Unit ={
    val sales: DataFrame = Seq(
      ("Beijing", 2016, 100),
      ("Beijing", 2017, 200),
      ("Shanghai", 2015, 50),
      ("Shanghai", 2016, 150),
      ("Guangzhou", 2017, 50)
    ).toDF("city", "year", "amount")

    //rollup 滚动分组
    sales.rollup('city,'year)
      .agg(sum('amount) as "amount")
      .sort('city asc_nulls_last,'year asc_nulls_last)
      .show()
  }

  @Test
  def rollup1(): Unit ={
    val schemaFinal = StructType(
      List(
        StructField("source", StringType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

    val pmFinal = spark.read
      .schema(schemaFinal)
      .option("header", value = true)
      .csv("dataset/pm_final.csv")

    pmFinal.rollup('source,'year)
      .agg(avg('pm) as "pm")
      .sort('source asc_nulls_last,'year asc_nulls_last)
      .show()
  }

  @Test
  def cube(): Unit ={
    val schemaFinal = StructType(
      List(
        StructField("source", StringType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

    val pmFinal = spark.read
      .schema(schemaFinal)
      .option("header", value = true)
      .csv("dataset/pm_final.csv")

    pmFinal.cube('source,'year)
      .agg(avg('pm) as "pm")
      .sort('source asc_nulls_last,'year asc_nulls_last)
      .show()
  }

  @Test
  def cubesql(): Unit ={
    val schemaFinal = StructType(
      List(
        StructField("source", StringType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

    val pmFinal = spark.read
      .schema(schemaFinal)
      .option("header", value = true)
      .csv("dataset/pm_final.csv")

    pmFinal.createOrReplaceTempView("pm_final")

    spark.sql("select source,year,avg(pm) as pm from pm_final group by source,year " +
      "grouping sets((source,year),(source),(year),())" +
      "order by source asc nulls last,year asc nulls last").show()
  }
}
