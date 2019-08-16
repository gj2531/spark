package sql

import org.apache.spark.sql.{ColumnName, Dataset, Row, SparkSession}
import org.junit.Test

class Column {
  private val spark: SparkSession = SparkSession.builder()
    .master("local[6]")
    .appName("column")
    .getOrCreate()

  import spark.implicits._

  @Test
  def creation(): Unit ={
    val ds: Dataset[Person] = Seq(Person("zhangsan",10),Person("lisi",20)).toDS
    val ds1: Dataset[Person] = Seq(Person("zhangsan",10),Person("lisi",20)).toDS
    val df: Dataset[Row] = Seq(("zhangsan",10),("lisi",20)).toDF("name","age")
    // ' 必须导入spark的隐式转换才能使用
    val column1: Symbol = 'name
    // $ 必须导入spark的隐式转换才能使用
    val column2: ColumnName = $"name"
    //col 必须导入functions才能使用
    import org.apache.spark.sql.functions._
    val column3 = col("name")

    //column 必须导入functions才能使用
    val column4 = column("name")

    ds.select(column1).show()
    //dataset使用算子时可以直接传入一个column或columnName对象 那么dataFrame也可以
    df.select(column1).show()
    //不止是select算子可以直接传入column对象 别的算子也可以
    df.where(column1 ==="zhangsan").show()

    //dataset.col
    val column5 = ds.col("name")
    val column6 = ds1.col("name")
    //这种select方式是错误的
    //使用    val column5 = ds.col("name")这种方式生成的column对象 会和dataset进行绑定
    ds.select(column6).show()
    //dataset.apply
    //column7==column8
    var column7 = ds.apply("name")
    val column8 = ds("name")
  }

  @Test
  def as(): Unit ={
    val ds: Dataset[Person] = Seq(Person("zhangsan",10),Person("lisi",20)).toDS
    //创建别名
    ds.select('name as "new_name").show()
    //类型转换
    ds.select('age.as[Double]).show()
  }

  @Test
  def columnapi(): Unit ={
    val ds: Dataset[Person] = Seq(Person("zhangsan",10),Person("lisi",20)).toDS

    //1.增加列 withColumn
    ds.withColumn("range",'age * 2).show()
    //2.like
    ds.filter('name like "%zh%").show()
    //3.isin
    ds.filter('name isin ("hello","zhangsan")).show()
    //4.sort
    ds.sort('age).show()
    ds.sort('age desc).show()

  }
}
