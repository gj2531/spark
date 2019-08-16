package spake_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class TransformationOp {

  val conf = new SparkConf().setMaster("local[6]").setAppName("transformation_op")
  val sc = new SparkContext(conf)

  @Test
  def mapPartitions: Unit = {
    //1.生成数据
    //2.使用算子
    //3.输出结果
    sc.parallelize(Seq(1,2,3,4,5,6),2)
      .mapPartitions(iter => {
        iter.foreach(item => println(item))
        iter
      })
      .collect()
  }
  @Test
  def mapPartitions2: Unit = {
    //1.生成数据
    //2.使用算子
    //3.输出结果
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6), 2)
      .mapPartitions(iter => {
        //将每一个元素*10返回
        //遍历iter
        iter.map(item => item * 10)
      })
      .collect()
      .foreach(item => println(item))
  }

  @Test
  def mapPartitionsWithIndex: Unit ={
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6), 2)
      .mapPartitionsWithIndex((index,iter) => {
        println("index:"+index)
        iter.foreach(item => println(item))
        iter
      })
      .collect()
  }

  @Test
  def filterDemo: Unit ={
    sc.parallelize(Seq(1,2,3))
      .filter(num => num >= 2)
      .foreach(item => println(item))
  }

  @Test
  def sampleDemo: Unit ={
    val rdd1: RDD[Int] = sc.parallelize(Seq(0,1,2,3,4,5,6,7,8,9))
    val rdd2: RDD[Int] = rdd1.sample(false,0.6)
    val result: Array[Int] = rdd2.collect()
    result.foreach(item =>println(item))
  }

  @Test
  def mapValue: Unit ={
    sc.parallelize(Seq(('a',1),('b',2),('c',3)))
      .mapValues(value => value * 10)
      .collect().foreach(item => println(item))
  }

  //交集intersection
  @Test
  def intersectionDemo: Unit ={
    val rdd1: RDD[Int] = sc.parallelize(Seq(1,2,3))
    val rdd2: RDD[Int] = sc.parallelize(Seq(3,4,5))
    rdd1.intersection(rdd2)
      .collect().foreach(item => println(item))
  }

  //并集union
  @Test
  def unionDemo: Unit ={
    val rdd1: RDD[Int] = sc.parallelize(Seq(1,2,3))
    val rdd2: RDD[Int] = sc.parallelize(Seq(3,4,5))
    rdd1.union(rdd2)
      .collect().foreach(item => println(item))
  }

  //差集 subtract
  @Test
  def subtractDemo: Unit ={
    val rdd1: RDD[Int] = sc.parallelize(Seq(1,2,3))
    val rdd2: RDD[Int] = sc.parallelize(Seq(3,4,5))
    rdd1.subtract(rdd2)
      .collect().foreach(item => println(item))
  }

  //groupByKey
  @Test
  def groupByKeyDemo: Unit ={
    sc.parallelize(Seq(('a',1),('a',1),('b',1)))
      .groupByKey()
      .collect().foreach(item => println(item))
  }

  //combinerByKey
  @Test
  def combinerByKey: Unit ={
    //1. 准备集合
    val rdd: RDD[(String, Double)] = sc.parallelize(Seq(
      ("zhangsan", 99.0),
      ("zhangsan", 96.0),
      ("lisi", 97.0),
      ("lisi", 98.0),
      ("zhangsan", 97.0))
    )
    //2.算子操作
    //2.1createCombiner转换数据
    //2.2mergevalue分区上的聚合
    //2.2mergeCombiners把所有分区上的数据再次聚合 生成最后结果
    val comBineResult: RDD[(String, (Double, Int))] = rdd.combineByKey(
      createCombiner = (curr: Double) => (curr, 1),
      mergeValue = (curr: (Double, Int), nextValue: Double) => (curr._1 + nextValue, curr._2 + 1),
      mergeCombiners = (curr: (Double, Int), agg: (Double, Int)) => (curr._1 + agg._1, curr._2 + agg._2)
    )
    val resultRdd: RDD[(String, Double)] = comBineResult.map(item => (item._1,item._2._1 / item._2._2))
    //3.获取结果 打印
    resultRdd.collect().foreach(item => println(item))
  }

  //foldBykey
  @Test
  def foldByKey: Unit ={
    sc.parallelize(Seq(("a", 1), ("a", 1), ("b", 1)))
      .foldByKey(10)((curr,agg) => curr+agg)
      .collect().foreach(println(_))
  }

  //aggregateByKey
  @Test
  def aggregateBykey: Unit ={
    val rdd = sc.parallelize(Seq(("手机", 10.0), ("手机", 15.0), ("电脑", 20.0)))
    val resultRdd: RDD[(String, Double)] = rdd.aggregateByKey(0.8)(
      seqOp = (dz, price) => (dz * price),
      combOp = (curr, agg) => (curr + agg)
    )
    resultRdd.collect().foreach(println(_))
  }

  //join
  @Test
  def join: Unit ={
    val rdd1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 1)))
    val rdd2 = sc.parallelize(Seq(("a", 10), ("a", 11), ("a", 12)))

    rdd1.join(rdd2)
      .collect().foreach(println(_))
  }

    //sortBy
  @Test
  def sortBy: Unit ={
    val rdd1: RDD[Int] = sc.parallelize(Seq(3,4,5,1,3,6))
    val rdd2: RDD[(String, Int)] = sc.parallelize(Seq(("a",1),("b",3),("c",2)))

    rdd1.sortBy(item =>item)

    rdd2.sortBy(item => item._2)

    rdd2.sortByKey()

    //按照value排序
    rdd2.map(item => (item._2,item._1)).sortByKey().map(item => (item._2,item._1)).collect().foreach(println(_))
  }

  //repartition coalesce
  @Test
  def partition: Unit ={
    val rdd1: RDD[Int] = sc.parallelize(Seq(1,2,3,4,5),2)

    //println(rdd1.repartition(5).partitions.size)
    //println(rdd1.repartition(1).partitions.size)
    println(rdd1.coalesce(5).partitions.length)
    println(rdd1.coalesce(1).partitions.length)
    println(rdd1.coalesce(5,shuffle = true).partitions.length)
  }
}
