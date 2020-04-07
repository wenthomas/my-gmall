package com.wenthomas.gmall.common

import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}

/**
 * ES工具类
 */
object ESUtil {

    //es服务地址:集群任一节点即可
    private val esUrl = "http://mydata01:9200"

    //客户端连接池
    val factory = new JestClientFactory
    val conf = new HttpClientConfig.Builder(esUrl)
            //允许最大客户端连接数
            .maxTotalConnection(100)
            //客户端连接服务器超时时间
            .connTimeout(10 * 1000)
            //读取数据的超时时间
            .readTimeout(10 * 1000)
            //多线程
            .multiThreaded(true)
            .build()
    factory.setHttpClientConfig(conf)


    /**
     * 向ES写入单条数据
     * @param indx index
     * @param source 数据源
     */
    def insertSingle(indx:String, source:Object, tp:String = "_doc", id:String = null) = {
        val client = factory.getObject

        val index = new Index.Builder(source)
                .index(indx)
                .`type`(tp)
                .id(id)
                .build()

        client.execute(index)
        client.shutdownClient()
    }

    /**
     * 向ES写入多条数据
     * @param indx
     * @param sources
     * @param tp
     */
    def insertBulk(indx:String, sources:Iterator[Object], tp:String = "_doc") = {
        val client = factory.getObject
        //插入多条的话，action应该包含多个doc
        var bulk = new Bulk.Builder()
                .defaultIndex(indx)
                .defaultType(tp)

        //循环插入数据：有id则插id，无id则自动生成
        sources.foreach({
            case (id:String, data) =>
                val action = new Index.Builder(data).id(id).build()
                bulk.addAction(action)
            case data =>
                val action = new Index.Builder(data).build()
                bulk.addAction(action)
        })

        //bulk封装完成并执行
        client.execute(bulk.build())
        client.shutdownClient()
    }

    /**
     *
     * @param args
     */
    def main(args: Array[String]): Unit = {
        val list = ("1001", User(20, "wenwen"))::("1002", User(30, "Verno"))::("1003", User(25, "Leo"))::Nil
        //todo
        insertBulk("user_info", list.toIterator)

    }

    /**
     * ES api代码解析
     */
    def learning() = {
        //1，先有es的客户端
        //1.1，创建一个工厂
        val factory = new JestClientFactory
        val conf = new HttpClientConfig.Builder(esUrl)
                //允许最大客户端连接数
                .maxTotalConnection(100)
                //客户端连接服务器超时时间
                .connTimeout(10 * 1000)
                //读取数据的超时时间
                .readTimeout(10 * 1000)
                //多线程
                .multiThreaded(true)
                .build()

        factory.setHttpClientConfig(conf)
        //1.2，从工厂获取一个客户端
        val client = factory.getObject

        //2，es需要的数据（json/样例类）
        /*        val data =
                    """
                        |{
                        |   "name":"wenthomas",
                        |   "age":20
                        |}
                        |""".stripMargin*/

        val data = User(30, "Verno")

        //3，向es写入数据
        //3.1 单次写入
        val index = new Index.Builder(data)
                //向哪个index插入数据
                .index("user_info")
                .`type`("_doc")
                //id(可选)：不传或传null则自动生成
                //.id("1")
                .build()

        client.execute(index)

        //4，关闭客户端(返还连接给工厂)
        client.shutdownClient()
    }
}

case class User(age:Int, name:String) {

}