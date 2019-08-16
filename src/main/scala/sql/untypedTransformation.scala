package sql

import org.apache.spark.sql.SparkSession
import org.junit.Test



class untypedTransformation {
  private val spark: SparkSession = SparkSession.builder()
    .master("local[6]")
    .appName("un type transformation")
    .getOrCreate()
  import spark.implicits._
  @Test
  def select(): Unit ={
    val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
    ds.select("name").show()
    ds.selectExpr("count(age)").show()

    import org.apache.spark.sql.functions._
    ds.select(expr("sum(age)")).show()
  }

  @Test
  def column(): Unit = {
    val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()

    import org.apache.spark.sql.functions._
    //生成新的列 列名为random
    ds.withColumn("random",rand(10)).show()
    //赋值name列的值 到新的列 新列列名为name_new
    ds.withColumn("name_new",'name).show()
    //name列的值与空字符串比较 比较的结果赋值到新的列 新列名为name_jok
    ds.withColumn("name_jok",'name === "").show()
    //name列重命名为new_name
    ds.withColumnRenamed("name","new_name").show()
  }

  @Test
  def drop(): Unit = {
    val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()

    ds.drop('name).show()
  }

  @Test
  def groupby(): Unit ={
    val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
    //groupByKey是有类型的 主要是因为groupByKey生成的对象里的算子的返回值是有类型的
    //而groupBy生成的对象里面的算子的返回值是没有类型的 主要是针对列进行操作的
    ds.groupBy('name).count().show()
  }

}
