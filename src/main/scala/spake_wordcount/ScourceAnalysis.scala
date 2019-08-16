package spake_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class ScourceAnalysis {
  val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("word_count")
  val sc = new SparkContext(conf)
  @Test
  def wordCount(): Unit ={
    //1.创建sc对象

    //2.读取文件
    val sourceRDD: RDD[String] = sc.textFile("dataset/wordcount.txt")
    //3.处理数据
    //    3.1拆词
    val splitRDD: RDD[String] = sourceRDD.flatMap(item => item.split(" "))
    //    3.2赋予初始词频
    val mapRDD: RDD[(String, Int)] = splitRDD.map(item => (item,1))
    //    3.3聚合统计个数
    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey((curr ,agg) => curr + agg)
    //    3.4将结果转换为字符串
    val resultRDD: RDD[String] = reduceByKeyRDD.map(item => s"${item._1}+${item._2}")
    //4.获取结果
    resultRDD.foreach(println(_))
    //5.关闭sc，执行
    sc.stop()
  }


  //需求：求得两个RDD之间的笛卡尔积
  @Test
  def narrowDepencyer(): Unit ={
    val rdd1: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5,6))
    val rdd2: RDD[String] = sc.parallelize(Seq("a","b","c"))
    val resultRDD: RDD[(Int, String)] = rdd1.cartesian(rdd2)
    resultRDD.collect().foreach(println(_))
    sc.stop()
  }
}
