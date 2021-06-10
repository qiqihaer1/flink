package com.symbio.study

import com.nx.state.utils.Utils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object sourceTest {


  //输入数据样例类
  case class ApacheLogEvent( ip: String, //IP地址
                             userId: String, //用户ID
                             eventTime: Long, //用户点击广告时间
                             method: String, //请求方式
                             url: String) //请求的URL

  // 窗口聚合结果样例类
  case class UrlViewCount( url: String, //请求的URL
                           windowEnd: Long,  //所属窗口
                           count: Long ) //点击的次数

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //必须引入隐式转换
    import org.apache.flink.streaming.api.scala._

    /**
     * 读取collection
     */
//    val list = List(
//      ApacheLogEvent("0.0.0.1", "A024", 1589681103000L, "GET", "/presentations/logstash-kafkamonitor-2020/images/kibana-search.png"),
//      ApacheLogEvent("83.149.9.123", "A024", 1589681104000L, "GET", "/presentations/logstash-kafkamonitor-2020/images/kibana-search.png"),
//      ApacheLogEvent("24.236.252.67", "A024", 1589681105000L, "GET", "/presentations/logstash-kafkamonitor-2020/images/kibana-search.png"),
//      ApacheLogEvent("110.136.166.128", "A024", 1589681103020L, "GET", "/presentations/logstash-kafkamonitor-2020/images/kibana-search.png"),
//      ApacheLogEvent("110.136.166.128", "A024", 1589681103001L, "GET", "/presentations/logstash-kafkamonitor-2020/images/kibana-search.png")
//    )
//    env.fromCollection(list).print()
    /**
     * 读取Any类型的数据
     */
//    env.fromElements(10,1.4,"hello",List("h","1","o")).print()
    /**
     * 读取textflie的数据
     */
//    env.readTextFile(Utils.eventLogPath).print()
    /**
     * 读取kafka的数据
     */
    new FlinkKafkaConsumer011[ApacheLogEvent]()

    env.addSource()



    env.execute("test01")

  }
}
