package com.symbio.study

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, RichFilterFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

import scala.util.Random


object TransformTest {

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
     * 读取kafka的数据
     * 已经创建了生产者：kafka-console-producer --broker-list vmnode01:9092 --topic from_flume01
     */
    val topic = "from_flume01"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","vmnode01:9092")//指定本机的HOST
    properties.setProperty("group.id","allTopic_consumer")
    properties.setProperty("auto.offset.reset", "latest")//可以不要
    val kafkaConsumer011 = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)


//    /**
//     * kafka+水印+keyby+window+min
//     */
//    env.addSource(kafkaConsumer011).map(x=> {
//      val obj = JSON.parseObject(x)
//      val value = json2TempLog(obj)
//      value
//    }).assignTimestampsAndWatermarks(new TimeExtractor) //设置水位，允许数据迟到10秒
//      .keyBy(_.country)
//      .timeWindow(Time.seconds(60),Time.seconds(15)) //设置窗口60s,滑动窗口5s
//      .minBy("temp").print()

//    /**
//     * kafka+reduce
//     */
//    env.addSource(kafkaConsumer011).map(x=> {
//      val obj = JSON.parseObject(x)
//      val value = json2TempLog(obj)
//      value
//    }).assignTimestampsAndWatermarks(new TimeExtractor) //设置水位，允许数据迟到10秒
//      .keyBy(_.country)
//      .timeWindow(Time.seconds(180),Time.seconds(60)) //设置窗口60s,滑动窗口5s
//      .reduce((last,now)=>{
//        //拿到当前key的最大温度
//        val flag = last.temp > now.temp
//        if(flag) last else now
//      }).print()

    /**
     * kafka+split+select
     */
    val splitStream: SplitStream[sourceTemperature] = env.addSource(kafkaConsumer011).map(x => {
      val obj = JSON.parseObject(x)
      val value = json2TempLog(obj)
      value
    }).keyBy(_.country).split(x => if (x.temp > 38) Seq("high") else Seq("low"))

    val highStream: DataStream[(String, Double)] = splitStream.select("high").map(x => (x.country, x.temp))
    val lowStream: DataStream[(String, Double)]= splitStream.select("low").map(x => (x.country, x.temp))

    highStream.connect(lowStream).map(new CoMapFunction[(String, Double),(String, Double), String] {

      var lowMap: util.HashMap[String, Double] = new util.HashMap[String, Double]

      override def map1(value: (String, Double)): String = {
        val StrBuffer = new StringBuffer

        val country = value._1
        if (lowMap.get(country)!=null) {
          StrBuffer+","+value._2.toString
        }
        country+","+StrBuffer
      }

//      def map2(value: (String, Double)){
//          this.lowMap.put(value._1 , value._2)
//      }
      override def map2(value: (String, Double)): String = {
        lowMap.put(value._1,value._2)
        ""
      }
    }).print()


    env.execute("test01")

  }


  def json2TempLog(json: JSONObject):sourceTemperature={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timeStamp = dateFormat.parse(json.get("dt").toString).getTime.toString
    val country = json.get("countryCode").toString
    val temp = json.get("temperature").toString.toDouble
    sourceTemperature(timeStamp.trim,country.trim,temp)
  }


  /**
   * 函数类测试
   * @param key
   */

  class MyFilter(key:String) extends FilterFunction[String] {
    override def filter(value: String): Boolean = {
      val strings = value.split(",")
      if (strings(0).contains(key)) true else false

    }
  }

  /**
   * 函数类和富函数类对比测试
   * @param key
   */
  class MyMap(key:String) extends MapFunction[String,String]{

    override def map(value: String)= "NO." + key
  }

  class MyRichMap(key:String) extends RichMapFunction[String,String]{
//    private val listState = ListState[Tuple2[String, String]]

//    override def open(parameters: Configuration): Unit = {
//      // 注册状态
//      val descriptor = new ListStateDescriptor[Tuple2[String, String]]("average", // 状态的名字
//        Types.TUPLE(Types.STRING, Types.STRING)) // 状态存储的数据类型
//
//      val stateList = getRuntimeContext.getListState(descriptor)
//    }

    override def map(value: String) ="NO." + key
  }


  /**
   * 定义waterMark
   */
  class TimeExtractor extends AssignerWithPeriodicWatermarks[sourceTemperature]{

    var currentMaxEventTime = 0L //设置当前窗口里面最大的时间
    val maxOufOfOrderness = 10000 //最大乱序时间 10s
    /**
     * 计算watermark
     * @return
     */
    override def getCurrentWatermark: Watermark = {
      new Watermark(currentMaxEventTime - maxOufOfOrderness)
    }

    /**
     * 指定我们的时间字段
     * @param element
     * @param previousElementTimestamp
     * @return
     */
    override def extractTimestamp(element: sourceTemperature, previousElementTimestamp: Long): Long = {
      //时间字段
      val timestamp = element.dt.toLong
      currentMaxEventTime = Math.max(element.dt.toLong, currentMaxEventTime)
      timestamp;
    }
  }


}
