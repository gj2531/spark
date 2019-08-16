package spake_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class ActionOp {

  val conf = new SparkConf().setMaster("local[6]").setAppName("transformation_op")
  val sc = new SparkContext(conf)

  @Test
  def reduce: Unit ={
    val rdd = sc.parallelize(Seq(("手机", 10.0), ("手机", 15.0), ("电脑", 20.0)))
    val result: (String, Double) = rdd.reduce((curr,agg) => ("总价:",curr._2+agg._2))
    println(result)
  }

  @Test
  def foreach(): Unit ={
    val rdd: RDD[Int] = sc.parallelize(Seq(1,2,3))
    rdd.foreach(println(_))
  }

  @Test
  def count(): Unit ={
    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a",1),("b",1),("c",1),("b",1)))
    println(rdd.count())
    println(rdd.countByKey())
    println(rdd.countByValue())
  }

  @Test
  def take(): Unit ={
    val rdd: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5))
    println(rdd.first())
    rdd.take(3).foreach(println(_))
    rdd.takeSample(withReplacement = true, num = 3).foreach(println(_))
  }

  //spark对数字类型的支持操作 除了这四种还有很多
  //这些对于数字的操作支持都是属于action
  @Test
  def numbeuic(): Unit ={
    val rdd: RDD[Int] = sc.parallelize(Seq(1,2,3,4,50,70,100))
    println(rdd.max())
    println(rdd.min())
    println(rdd.mean())
    println(rdd.sum())
  }
}
