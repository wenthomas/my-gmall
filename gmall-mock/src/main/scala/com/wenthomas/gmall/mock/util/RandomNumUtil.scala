package com.wenthomas.gmall.mock.util

import java.util.Random

import scala.collection.mutable


/**
 * @author Verno
 * @create 2020-03-24 21:36 
 */
object RandomNumUtil {
    private val random = new Random()

    /**
     * 返回一个随机整数[from, to]
     * @param from
     * @param to
     * @return
     */
    def randomInt(from: Int, to: Int): Int = {
        if (from > to) throw new IllegalArgumentException(s"from=$from 应该小于 to=$to")
        //[0, to - from) + from [from, to - from + from]
        random.nextInt(to - from + 1) + from
    }

    /**
     * 返回一个随机Long[from, to]
     * @param from
     * @param to
     * @return
     */
    def randomLong(from: Long, to: Long): Long = {
        if (from > to) throw new IllegalArgumentException(s"from=$from 应该小于 to=$to")
        random.nextLong().abs % (to - from + 1) + from
    }

    /**
     * 生成一系列的随机值
     * @param from
     * @param to
     * @param count
     * @param canRepeat
     * @return
     */
    def randomMultiInt(from: Int, to: Int, count: Int, canRepeat: Boolean = true): List[Int] = {
        if (canRepeat) {
            (1 to count).map(_ => randomInt(from, to)).toList
        } else {
            val set = mutable.Set[Int]()
            while (set.size < count) {
                set += randomInt(from, to)
            }
            set.toList
        }
    }

    /**
     * 测试方法
     * @param args
     */
    def main(args: Array[String]): Unit = {
        println(randomMultiInt(1, 15, 10))
        println("---------------------------------------------------------------------------------")
        println(randomMultiInt(1, 8, 6,false))
    }
}
