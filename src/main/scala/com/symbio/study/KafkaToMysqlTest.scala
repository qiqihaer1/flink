package com.symbio.study

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.nx.state.lesson01.HotPageEventTimeExtractor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema


object KafkaToMysqlTest {


  //输入数据样例类
  case class sourceTemperature(dt: String,
                               country: String,
                               temp: Double)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(3)//设置并行度
    println(env.getParallelism)

    /**
     * 设置checkpoint
     */
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


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

    /**
     * kafka+keyby+min
     */
    val stream = env.addSource(kafkaConsumer011)
      .map(json2TempretureLog(_))
      .assignTimestampsAndWatermarks(new TimeExtractor) //设置水位，允许数据迟到10秒
      .keyBy(_.country)
      .timeWindow(Time.seconds(60),Time.seconds(30)) //设置窗口,窗口时间为60s，滑动时间为30s
      .minBy("temp")

    /**
     * sink = kafka
     */
    val producerTopic = "out_data"
    val producerProperties = new Properties()
    producerProperties.setProperty("bootstrap.servers","vmnode01:9092")//指定本机的HOST
    val kafkaProducer011 = new FlinkKafkaProducer011[String](producerTopic, new SimpleStringSchema(), producerProperties)
    stream.map(_.toString).addSink(kafkaProducer011)

//
//    /**
//     * sink = mysql
//     */
//    stream.addSink(new mysqlSinkFunc)


    env.execute("sink01")

  }



  def json2TempretureLog(jsonStr: String):sourceTemperature={
    val json = JSON.parseObject(jsonStr)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timeStamp = dateFormat.parse(json.get("dt").toString).getTime.toString
    val country = json.get("countryCode").toString
    val temp = json.get("temperature").toString.toDouble
    sourceTemperature(timeStamp.trim,country.trim,temp)
  }



  class mysqlSinkFunc extends RichSinkFunction[sourceTemperature]{
     var connection: Connection = _
      var psInsert : PreparedStatement = _
      var psUpdate : PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      Class.forName("com.mysql.cj.jdbc.Driver")
      val url = "jdbc:mysql://vmmaster:3306/bigdata?useUnicode=true&characterEncoding=UTF-8"
      val username = "root"
      val password = "Symbio@123"
      this.connection = DriverManager.getConnection(url, username, password)
      val sqlInsert = "insert into sensor_log(country,temperature) values(?,?)"
      psInsert = this.connection.prepareStatement(sqlInsert)
      val sqlUpdate = "update sensor_log set temperature=? where country=?"
      psUpdate = this.connection.prepareStatement(sqlUpdate)
      println("mysql建立连接")
    }

    override def invoke(value: sourceTemperature): Unit = {
        psUpdate.setString(1,value.temp.toString)
        psUpdate.setString(2,value.country)
        psUpdate.execute()
        //如果没有查到数据，就插入
        if(psUpdate.getUpdateCount==0){
          psInsert.setString(1,value.country)
          psInsert.setString(2,value.temp.toString)
          psInsert.execute()
        }
      println("mysql被调用")
    }

    override def close(): Unit = {
      psInsert.close()
      psUpdate.close()
      connection.close()
    }
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
      timestamp
    }
  }


}
