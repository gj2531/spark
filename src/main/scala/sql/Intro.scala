package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.junit.Test

class Intro {
  @Test
  def rddIntro(): Unit ={
    val conf = new SparkConf().setAppName("rdd intro").setMaster("local[6]")
    val sc = new SparkContext(conf)
    sc.textFile("dataset/wordcount.txt")
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .collect()
  }


  @Test
  def dsIntro(): Unit ={

    val spark =new SparkSession.Builder()
      .master("local[6]")
      .appName("ds intro")
      .getOrCreate

    import spark.implicits._

    val sourceRDD: RDD[Person] = spark.sparkContext.parallelize(Seq(Person("zs",10),Person("ls",15)))
    val personDS: Dataset[Person] = sourceRDD.toDS()
    val resultDS: Dataset[String] = personDS.where('age > 10)
      .where('age < 20)
      .select('name)
      .as[String]
    resultDS.show()
  }

  @Test
  def dfIntor(): Unit ={
    val spark =new SparkSession.Builder()
      .master("local[6]")
      .appName("ds intro")
      .getOrCreate

    import spark.implicits._

    val sourceRDD: RDD[Person] = spark.sparkContext.parallelize(Seq(Person("zs",10),Person("ls",15)))
    val df: DataFrame = sourceRDD.toDF()

    df.createOrReplaceTempView("person")

    val resultDF: DataFrame = spark.sql("select * from person where age > 10 and age < 20")
    resultDF.show()
  }

  @Test
  def dataset1(): Unit ={
    val spark = new sql.SparkSession.Builder()
      .master("local[6]")
      .appName("data set1")
      .getOrCreate()

    import spark.implicits._

    val sourceRDD: RDD[Person] = spark.sparkContext.parallelize(Seq(Person("zs",10),Person("ls",15)))
    val dataset: Dataset[Person] = sourceRDD.toDS()
    //支持强类型API
    dataset.filter(item => item.age > 10).show()
    //支持弱类型API
    dataset.filter('age > 10).show()
    dataset.filter($"age" > 10).show()
    //可以直接编写sql表达式
    dataset.filter("age > 10").show()
  }

  @Test
  def dataFrame(): Unit ={
    //1.创建sparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("data frame")
      .getOrCreate()

    import spark.implicits._

    //2.获得dataFrame
    val dataFrame: DataFrame = Seq(Person("lisi",15),Person("wangwu",20)).toDF()

    //3.使用dataFrame
    dataFrame.where('age > 15)
      .select('name)
      .show()

  }

  @Test
  def dataFrame1(): Unit ={
    //1.创建sparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("data frame")
      .getOrCreate()

    import spark.implicits._

    //2.获得dataFrame
    val personlist = Seq(Person("lisi",15),Person("wangwu",20))

    //创建dataFrame的三种方式
    //1.todf
    val df1: DataFrame = personlist.toDF()
    val df2: DataFrame = spark.sparkContext.parallelize(personlist).toDF()
    //2.createDataFrame
    val df3: DataFrame = spark.createDataFrame(personlist)
    //3.dataFrameReader
    val df4: DataFrame = spark.read.csv("dataset/BeijingPM20100101_20151231_noheader.csv")
  }

  @Test
  def dataFrame2(): Unit ={
    //1.创建sparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("beijing apm")
      .getOrCreate()

    //导入隐式转换


    //2.读取数据
    val sourceDF: DataFrame = spark.read
      .option("header", value = true)
      .csv("dataset/BeijingPM20100101_20151231.csv")

    //sourceDF.printSchema()
    import spark.implicits._

    //3.处理数据
    //select year,month,count(PM_Dongsi) from ... where PM_Dongsi != "NA" group by year,month
      //3.1选择列
      //3.2过滤掉PM_Dongsi为NA的值
      //3.3分组
      //3.4聚合
      //3.5得出结论
//    sourceDF.select('year ,'month ,'PM_Dongsi)
//      .where('PM_Dongsi =!= "NA")
//      .groupBy('year , 'month)
//      .count()
//      .show()
    sourceDF.createOrReplaceTempView("PM_BEIJING")
    val resultDF: DataFrame = spark.sql("select year,month,count(PM_Dongsi) from PM_BEIJING where PM_Dongsi != 'NA' group by year,month")
    resultDF.show()
    spark.stop()
  }
}

case class Person(name:String,age:Int)
