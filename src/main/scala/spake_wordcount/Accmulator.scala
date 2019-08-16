package spake_wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class Accmulator {

  @Test
  def acc(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("acc")
    val sc = new SparkContext(conf)

    val numAcc= new numAccmulator
    sc.register(numAcc,"numAcc")

    sc.parallelize(Seq("1","2","3"))
      .foreach(numAcc.add(_))

    println(numAcc.value)
  }
}

class numAccmulator extends AccumulatorV2[String,Set[String]] {
  //定义一个最终结果
  private val nums : mutable.Set[String] = mutable.Set()
  //是否为空
  override def isZero: Boolean = {
    nums.isEmpty
  }
  //提供给spark框架一个复制的累加器
  override def copy(): AccumulatorV2[String, Set[String]] = {
    val numAccmulator = new numAccmulator
    nums.synchronized{
      numAccmulator.nums ++= this.nums
    }
    numAccmulator
  }
  //帮助spark框架清理累加器的内容
  override def reset(): Unit = {
    nums.clear()
  }

  //外部传入要累加的内容 在这个方法内进行累加
  override def add(v: String): Unit = {
    nums += v
  }

  //累加器在进行累加的时候 可能在每个分布式节点都有一个实例
  //这个方法的作用就是最后在driver进行一次合并 把所有的实例中的内容合并起来
  override def merge(other: AccumulatorV2[String, Set[String]]): Unit = {
    nums ++= other.value
  }

  //提供给外部累加结果
  //一定要是不可变的 因为外部有可能再进行修改 如果是可变的 外部的修改会影响内部的值
  override def value: Set[String] = {
    nums.toSet
  }
}
