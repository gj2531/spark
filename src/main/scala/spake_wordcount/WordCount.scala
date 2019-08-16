package spake_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建Spark Context
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("wordCount")
    val sparkContext = new SparkContext(sparkConf)
    //2.读取文件并计算词频
    val rdd1: RDD[String] = sparkContext.textFile("dataset/wordcount.txt")
    val rdd2: RDD[String] = rdd1.flatMap(item => item.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map(item => (item,1))
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((curr,agg) => curr+agg)
    val result: Array[(String, Int)] = rdd4.collect()
    //3.查看执行结果
    result.foreach(println(_))
  }
}
