package spake_wordcount

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class AccessLogAgg {
  @Test
  def ipAgg: Unit ={
    //todo 1. 创建SparkContext
    val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("ip_agg")
    val sc = new SparkContext(conf)
    //todo 2. 读取文件，生成数据集
    val sourceRdd: RDD[String] = sc.textFile("dataset/access_log_sample.txt")
    //3.todo  取出IP，赋予出现次数1
    val ipRdd: RDD[(String, Int)] = sourceRdd.map(item => (item.split(" ")(0),1))
    //4.todo  简单清洗
        //4.1 去掉空的数据 4.2 去掉非法的数据 4.3 根据业务调整数据
    val cleanRdd: RDD[(String, Int)] = ipRdd.filter(item => StringUtils.isNotEmpty(item._1))

    //5.todo  根据IP出现次数进行聚合
    val ipAggRdd: RDD[(String, Int)] = cleanRdd.reduceByKey((curr , agg) => curr + agg)
    //6.todo  根据IP出现次数进行排序
    val sortedRdd: RDD[(String, Int)] = ipAggRdd.sortBy(item => item._2,ascending = false)
    //7.todo  取出结果 打印结果
    sortedRdd.take(10).foreach(println(_))

  }
}
