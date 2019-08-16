package spake_wordcount

import org.junit.Test

class Closure {


  //闭包
  //编写一个高阶函数 有一个变量 返回一个函数 通过这个变量完成计算
  @Test
  def test(): Unit ={
    var f: Int => Double = closure()
    println(f(3))
  }

  def closure(): Int => Double ={
    val pi = 3.14
    val areaFunc = (x:Int) => math.pow(x,2) * pi
    areaFunc
  }
}
