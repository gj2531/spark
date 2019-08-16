package spake_wordcount

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class StagePartice {
  val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("pm_process")
  val sc = new SparkContext(conf)
  @Test
  def pmProcess(): Unit ={
    //1.创建sparkContext对象

    //2.读取文件
    val source: RDD[String] = sc.textFile("dataset/BeijingPM20100101_20151231_noheader.csv")
    //3.处理数据
        //3.1抽取数据
    val result: RDD[((String, String), Int)] = source.map(item => ((item.split(",")(1), item.split(",")(2)), item.split(",")(6)))
      //3.2清洗数据
      .filter(item => StringUtils.isNotEmpty(item._2) && !item._2.equalsIgnoreCase("na"))
      .map(item => (item._1, item._2.toInt))
      //3.3聚合
      .reduceByKey((curr, agg) => curr + agg)
      //3.4排序
      .sortBy(item => item._2,ascending = false)

    //4.获取结果
    result.take(10).foreach(println(_))
    //5.运行测试
    sc.stop()
  }

  //查看有多少分区
  @Test
  def par(): Unit ={
    val rdd1: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5,6,7,8,9,10))
    println(rdd1.partitions.size)
  }
}
