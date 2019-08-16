package sql

import java.lang

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.junit.Test


class TypedTransformation {
  //case class Person(name:String,age:Int)
  //1.创建sparkSession
  private val spark: SparkSession = SparkSession.builder()
    .master("local[6]")
    .appName("typed transformation")
    .getOrCreate()
  import spark.implicits._
  @Test
  def trans(): Unit ={

    import spark.implicits._
    //2. 创建数据集
    val ds: Dataset[String] = Seq("hello world","spark nihao").toDS()
    val ds1: Dataset[Person] = Seq(Person("zhangsan",10),Person("lisi",20)).toDS()

    //3.flatmap
    ds.flatMap(_.split(" ")).show()

    //4.map
    ds1.map(person => Person(person.name,person.age * 2)).show()
    //5.mapPartitions
    ds1.mapPartitions(
      iter => {
        val result= iter.map(person => Person(person.name,person.age * 2))
        result
      }
    ).show()
  }

  @Test
  def trans1(): Unit ={
    import spark.implicits._
    val ds: Dataset[lang.Long] = spark.range(10)
    ds.transform(dataset => dataset.withColumn("doubled",'id * 2))
      .show()
  }

  //as方法本质上就是把dataset[Row].as => dataset[Student]
  @Test
  def as(): Unit ={
    //class Student(name:String,age:Int,gpa:Float)
    import spark.implicits._
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("gpa", FloatType)
      )
    )
    val sourceDF: DataFrame = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .csv("dataset/studenttab10k")

    val ds: Dataset[Student] = sourceDF.as[Student]
    ds.show()
  }

  @Test
  def filter(): Unit ={
    import spark.implicits._
    val ds: Dataset[Person] = Seq(Person("zhangsan",10),Person("lisi",20)).toDS()
    ds.filter(_.age > 10).show()
  }

  @Test
  def groupbykey(): Unit ={
    val ds: Dataset[Person] = Seq(Person("zhangsan",10),Person("lisi",20)).toDS()
    ds.groupByKey(person => person.name).count().show()
  }

  @Test
  def randomSplit(): Unit ={
    val ds: Dataset[lang.Long] = spark.range(15)
    val ds1: Array[Dataset[lang.Long]] = ds.randomSplit(Array[Double](2,3))

    ds1.foreach(_.show())
  }

  @Test
  def sample(): Unit ={
    val ds: Dataset[lang.Long] = spark.range(15)
    ds.sample(withReplacement = true,fraction = 0.4).show()
  }

  @Test
  def sorted(): Unit ={
    val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
    //orderBy
    ds.orderBy("age").show()
    ds.orderBy('age desc).show()

    //sort
    ds.sort("name").show()
    ds.sort('name.desc).show()
  }


  //去重
  @Test
  def dropDuplicates(): Unit ={
    val ds = spark.createDataset(Seq(Person("zhangsan", 15), Person("zhangsan", 15), Person("lisi", 15)))
    ds.dropDuplicates("age").show()

    ds.distinct().show()

  }

  //集合操作
  //except except 和 SQL 语句中的 except 一个意思, 是求得 ds1 中不存在于 ds2 中的数据, 其实就是差集
  //intersect 求得两个集合的交集
  //union 求得两个集合的并集
  //limit 限制结果集数量
  @Test
  def collection(): Unit ={
    val ds1 = spark.range(1, 10)
    val ds2 = spark.range(5, 15)
    ds1.except(ds2).show()//差集
    ds1.intersect(ds2).show()//交集
    ds1.union(ds2)//并集
    ds1.limit(10).show()//限制结果集数量 比如只想查看前几行
  }

}
case class Student(name:String,age:Int,gpa:Float)
