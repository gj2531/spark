package spake_wordcount

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class CacheOp {

  private val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("cache_op")
  private val sc = new SparkContext(conf)

  /**
    * 1.创建sc
    * 2.读取文件
    * 3.取出IP,赋予初始词频
    * 4.清洗
    * 5.统计IP出现的次数
    * 6.统计出现次数最少的IP
    * 7.统计出现次数最多的IP
    */
  @Test
  def propare(): Unit ={
    val source: RDD[String] = sc.textFile("dataset/access_log_sample.txt")
    val countRdd: RDD[(String, Int)] = source.map(item => (item.split(" ")(0), 1))
    val cleanRdd: RDD[(String, Int)] = countRdd.filter(item => StringUtils.isNotEmpty(item._1))
    val aggRdd: RDD[(String, Int)] = cleanRdd.reduceByKey((curr, agg) => curr + agg)

    val lessIp: (String, Int) = aggRdd.sortBy(item => item._2,ascending = true).first()
    val moreIp: (String, Int) = aggRdd.sortBy(item => item._2,ascending = false).first()

    println(lessIp,moreIp)

  }


  //todo RDD缓存
  //todo cache()/persist()/persist(存储级别，默认是StorageLevel.MEMORY_ONLY)
  //todo cache() == persist()

  @Test
  def cache(): Unit ={
    val source: RDD[String] = sc.textFile("dataset/access_log_sample.txt")
    val countRdd: RDD[(String, Int)] = source.map(item => (item.split(" ")(0), 1))
    val cleanRdd: RDD[(String, Int)] = countRdd.filter(item => StringUtils.isNotEmpty(item._1))
    var aggRdd: RDD[(String, Int)] = cleanRdd.reduceByKey((curr, agg) => curr + agg)

    aggRdd = aggRdd.cache()

    val lessIp: (String, Int) = aggRdd.sortBy(item => item._2,ascending = true).first()
    val moreIp: (String, Int) = aggRdd.sortBy(item => item._2,ascending = false).first()

    println(lessIp,moreIp)

  }

  @Test
  def chechPoint(): Unit ={
    val source: RDD[String] = sc.textFile("dataset/access_log_sample.txt")
    sc.setCheckpointDir("checkPoint")
    val countRdd: RDD[(String, Int)] = source.map(item => (item.split(" ")(0), 1))
    val cleanRdd: RDD[(String, Int)] = countRdd.filter(item => StringUtils.isNotEmpty(item._1))
    var aggRdd: RDD[(String, Int)] = cleanRdd.reduceByKey((curr, agg) => curr + agg)

    aggRdd = aggRdd.cache()
    aggRdd.checkpoint()

    val lessIp: (String, Int) = aggRdd.sortBy(item => item._2,ascending = true).first()
    val moreIp: (String, Int) = aggRdd.sortBy(item => item._2,ascending = false).first()

    println(lessIp,moreIp)

  }
}
