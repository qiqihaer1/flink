package com.symbio.study

import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneId}
import java.util
import java.util.{HashMap, Properties}

import akka.event.LogSource
import com.alibaba.fastjson.{JSON, JSONObject}
import com.nx.state.lesson01.{ApacheLogEvent, HotPageEventTimeExtractor}
import com.nx.state.utils.Utils
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import scala.collection.mutable
import scala.util.Random


object sourceTest {

  //输入数据样例类1
  case class sourceTestLogEvent( ip: String, //IP地址
                                 userId: String, //用户ID
                                 eventTime: Long, //用户点击广告时间
                                 method: String, //请求方式
                                 url: String) //请求的URL
  //输入数据样例类2
  case class sourceTemperature(dt: String,
                               country: String,
                               temp: Double)

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
//      sourceTemperature("0.0.0.1", "A024", 1589681103000L, "GET", "/presentations/logstash-kafkamonitor-2020/images/kibana-search.png"),
//      sourceTemperature("83.149.9.123", "A024", 1589681104000L, "GET", "/presentations/logstash-kafkamonitor-2020/images/kibana-search.png"),
//      sourceTemperature("24.236.252.67", "A024", 1589681105000L, "GET", "/presentations/logstash-kafkamonitor-2020/images/kibana-search.png"),
//      sourceTemperature("110.136.166.128", "A024", 1589681103020L, "GET", "/presentations/logstash-kafkamonitor-2020/images/kibana-search.png"),
//      sourceTemperature("110.136.166.128", "A024", 1589681103001L, "GET", "/presentations/logstash-kafkamonitor-2020/images/kibana-search.png")
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
     * 读取自定义源的数据
     * 已经创建了生产者：kafka-console-producer --broker-list vmnode01:9092 --topic from_flume01
     */
    //        env.addSource(new LogSource).print()

    /**
     * 读取kafka的数据
     * 已经创建了生产者：kafka-console-producer --broker-list vmnode01:9092 --topic from_flume01
     */
    val topic = "from_flume01"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","vmnode01:9092")//指定本机的HOST
    properties.setProperty("group.id","allTopic_consumer")
    properties.setProperty("auto.offset.reset", "latest")//可以不要
    val kafkaConsumer011 = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)

//    env.addSource(kafkaConsumer011).print()


    env.execute("test01")

  }



}
