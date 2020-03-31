package com.wenthomas.gmall.mock.util

import scala.collection.mutable.ListBuffer

/**
 * @author Verno
 * @create 2020-03-24 21:50 
 */
/**
 *用于生成带有比重的随机选项：
 * 根据提供的值和比重, 来创建RandomOptions对象.
 * 然后可以通过getRandomOption来获取一个随机的预定义的值
 */
object RandomOptions {
    def apply[T](opts: (T, Int)*): RandomOptions[T] = {
        val randomOptions = new RandomOptions[T]()
        //计算出来总的比重
        randomOptions.totalWeight = (0 /: opts) (_ + _._2)
        opts.foreach({
            case (value, weight) => randomOptions.options ++= (1 to weight).map(_ => value)
        })
        randomOptions
    }

    def main(args: Array[String]): Unit = {
        val opts = RandomOptions(("张三", 10), ("李四", 30), ("王五", 20))
        println(opts.getRandomOption())
        println(opts.getRandomOption())
        println(opts.getRandomOption())
        println(opts.getRandomOption())
        println(opts.getRandomOption())
    }
}

//比重：工程师10 程序员10 老师20
class RandomOptions[T] {
    var totalWeight: Int = _
    var options: ListBuffer[T] = ListBuffer[T]()

    /**
     * 获取随机的Option的值
     * @return
     */
    def getRandomOption() = {
        options(RandomNumUtil.randomInt(0, totalWeight - 1))
    }
}