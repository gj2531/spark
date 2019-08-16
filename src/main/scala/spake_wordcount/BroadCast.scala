package spake_wordcount

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class BroadCast {

  private val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("broad_cast")
  private val sc = new SparkContext(conf)

  @Test
  def bc1(): Unit ={
    val v = Map("hadoop" -> "www.hadoop.org.cn","scala" -> "www.scala.lang.org")

    val result: Array[String] = sc.parallelize(Seq("hadoop", "scala"))
      .map(item => v(item)).collect()
    result.foreach(println(_))
  }

  @Test
  def bc2(): Unit ={
    val v = Map("hadoop" -> "www.hadoop.org.cn","scala" -> "www.scala.lang.org")

    val bc: Broadcast[Map[String, String]] = sc.broadcast(v)
    val result: Array[String] = sc.parallelize(Seq("hadoop", "scala"))
      .map(item => bc.value(item)).collect()
    result.foreach(println(_))
  }
}
