package com.symbio.study

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util
import java.util.{HashMap, List, Properties}

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests


object SinkTest {

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

    /**
     * kafka+keyby+min
     */
    val stream = env.addSource(kafkaConsumer011).map(json2TempretureLog(_)).keyBy(_.country).minBy("temprature")

    /**
     * sink = csv
     */
//    stream.writeAsCsv("D:\\yingxu.liao\\nx-flink-state\\src\\main\\resources\\data_sink_test1.csv")
//
//    stream.addSink(
//      StreamingFileSink.forRowFormat(
//      new Path("D:\\yingxu.liao\\nx-flink-state\\src\\main\\resources\\data_sink_test2.csv"),
//      new SimpleStringEncoder[sourceTemperature]()
//      ).build()
//    )
    /**
     * sink = kafka
     */
//    val producerTopic = "out_data"
//    val producerProperties = new Properties()
//    producerProperties.setProperty("bootstrap.servers","vmnode01:9092")//指定本机的HOST
//    val kafkaProducer011 = new FlinkKafkaProducer011[String](producerTopic, new SimpleStringSchema(), producerProperties)
//    stream.map(_.toString).addSink(kafkaProducer011)
    /**
     * sink = redis
     */
//    val redisConf = new FlinkJedisPoolConfig.Builder()
//      .setHost("localhost")
//      .setPort(6379)
//        .build()
//
//    stream.addSink(new RedisSink[sourceTemperature](redisConf,new RedisMapper[sourceTemperature] {
//      //定义保存数据写入redis的命令，Hset 表名 key,value
//      override def getCommandDescription: RedisCommandDescription =new RedisCommandDescription(RedisCommand.HSET,"sensor_tmep")
//      //将id指定为key
//      override def getKeyFromData(data: sourceTemperature): String = data.country
//      //将温度值指定为value
//      override def getValueFromData(data: sourceTemperature): String = data.temprature.toString
//    }))
    /**
     * sink = es
     */
//    val httpHosts =new util.ArrayList[HttpHost]
//    httpHosts.add(new HttpHost("locahost",9200))
//    val elasticsearchSink = new ElasticsearchSink.Builder[sourceTemperature](httpHosts, new ElasticsearchSinkFunction[sourceTemperature] {
//      override def process(element: sourceTemperature, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
//        //包装数据作为data source
//        val country = element.country
//        val temprature = element.temprature
//        val dt = element.dt
//        val map = new util.HashMap[String, String]()
//        map.put("country",country)
//        map.put("temperature",temprature.toString)
//        map.put("dt",dt)
//        //创建index request,用于发送http请求
//        val request = Requests.indexRequest()
//          .index("sensor") //_index字段  /sensor/_search?xxx
//          .`type`("readingdata")//_type字段
//          .source(map)//插入内容
//        //用indexer 发送请求
//        indexer.add(request)
//      }
//    }).build()
//
//    stream.addSink(elasticsearchSink)

    /**
     * sink = mysql
     */
    stream.addSink(new mysqlSinkFunc)


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
      timestamp;
    }
  }


}
