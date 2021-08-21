package com.symbio.study

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
//import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema


object KafkaToMysqlTest {

  //输入数据样例类
  case class sourceTemperature(timeStamp: Long,
                               dt: String,
                               country: String,
                               temprature: Double)

  //转换样例类
  def json2Log(jsonStr: String): sourceTemperature = {
    val json = JSON.parseObject(jsonStr)
    val format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    //    val timeStamp = LocalDateTime.parse(json.get("dt").toString,format).toEpochSecond(ZoneOffset.of("+8"))大错特错，这个是秒时间戳
    //    val timeStamp = LocalDateTime.parse(json.get("dt").toString,format).toInstant(ZoneOffset.of("+8")).toEpochMilli()

    val timeStamp = json.get("timestamp").toString.toLong

    import java.time.LocalDateTime
    import java.time.ZoneOffset
    val dt = LocalDateTime.ofEpochSecond(timeStamp, 0, ZoneOffset.ofHours(8))
    val country = json.get("countryCode").toString
    val temp = json.get("temperature").toString.toDouble
    sourceTemperature( timeStamp, dt.toString.trim, country.trim, temp)
  }

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //设置并行度
    println(env.getParallelism)
//    env.setStateBackend(new FsStateBackend("hdfs://vmmaster:8020/flink/checkpoint"))

    /**
     * 设置checkpoint
     */
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) //exactly-once是默认值
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      3, //尝试重启的次数
     10000//间隔
    )) //重启策略

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //必须引入隐式转换
    import org.apache.flink.streaming.api.scala._

    /**
     * 读取kafka的数据
     * 已经创建了生产者：kafka-console-producer --broker-list vmnode01:9092 --topic from_flume01
     */
    val topic = "from_flume1"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "vmnode01:9092") //指定本机的HOST
    properties.setProperty("group.id", "allTopic_consumer")
    properties.setProperty("auto.offset.reset", "latest") //可以不要
    val kafkaConsumer011 = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)

    env.addSource(kafkaConsumer011)
      .map(json2Log(_))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[sourceTemperature](Time.seconds(1)) {
      override def extractTimestamp(element: sourceTemperature): Long = element.timeStamp
    })
      .keyBy(x => x.country)
      .timeWindow(Time.hours(1))
      .trigger(new MyCountTrigger)
        .aggregate(new AggregateFunction[sourceTemperature,Long,Long] {
          override def createAccumulator(): Long = 0L

          override def add(value: sourceTemperature, accumulator: Long): Long = accumulator+1

          override def getResult(accumulator: Long): Long = {
//            println("累加值："+accumulator)
            accumulator
          }

          override def merge(a: Long, b: Long): Long = a+b
        },
          new WindowFunction[Long,(String,String,String,Long),String,TimeWindow] {
            override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[(String,String,String,Long)]): Unit = {
              val windowEnd = window.getEnd
              val format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
              val dateStr = format.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(windowEnd), ZoneId.systemDefault()))
              val now = LocalDateTime.now().toString

              out.collect((dateStr,now,key,input.iterator.next()))
            }
          })
        .print()
//      .reduce((cur, newDate) => sourceTemperature(cur.timeStamp, cur.dt, cur.country, cur.temprature.min(newDate.temprature)))

//    ds.print()
//    ds.process().addSink(new mysqlSinkFunc)

    /**
     * kafka+keyby+min
     */
//    val stream = env.addSource(kafkaConsumer011)
//      .map(json2Log(_))
//      .assignTimestampsAndWatermarks(new TimeExtractor) //设置水位，允许数据迟到10秒
//      .keyBy(_.country)
//      .timeWindow(Time.seconds(60))
//      //      .minBy("temp")
//      .reduce((cur, newDate) => sourceTemperature(cur.dt, cur.country, cur.temprature.min(newDate.temprature)))

    //    /**
    //     * sink = kafka
    //     */
    //    val producerTopic = "out_data"
    //    val producerProperties = new Properties()
    //    producerProperties.setProperty("bootstrap.servers", "vmnode01:9092") //指定本机的HOST
    //    val kafkaProducer011 = new FlinkKafkaProducer011[String](producerTopic, new SimpleStringSchema(), producerProperties)
    //    stream.map(_.toString).addSink(kafkaProducer011)


    /**
     * sink = mysql
     */
    //    //mysql的第一种写法，自定义richSinkFunction和checkpointFunction
    //    stream.addSink(new mysqlSinkFunc)
    //mysql的第二种写法
    //set the types，import java.sql.Types 不然报错Unknown column type for column %s. Best effort approach to set its value: %s.
//    val sqlInsert = "insert into sensor_log(country,temperature) values(?,?)"
//    val jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
//      .setDrivername("com.mysql.jdbc.Driver")
//      .setDBUrl("jdbc:mysql://vmmaster:3306/bigdata?useUnicode=true&characterEncoding=UTF-8")
//      .setUsername("root")
//      .setPassword("Symbio@123")
//      .setQuery(sqlInsert)
//      .setSqlTypes(Array[Int](Types.VARCHAR, Types.DOUBLE))
//      .finish()

//    ds.map(x => {
//      val row = new Row(4)
//      row.setField(0, x.timeStamp)
//      row.setField(1, x.dt)
//      row.setField(2, x.country)
//      row.setField(3, x.temprature)
//      row
//      val row = new Row(2)
//      row.setField(0, x.country)
//      row.setField(1, x.temprature)
//      row
//    }).writeUsingOutputFormat(jdbcOutput)


    env.execute("sink01")

  }




  class mysqlSinkFunc extends RichSinkFunction[sourceTemperature] {
    var connection: Connection = _
    var psInsert: PreparedStatement = _
    var psUpdate: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      Class.forName("com.mysql.cj.jdbc.Driver")
      val url = "jdbc:mysql://vmmaster:3306/bigdata?useUnicode=true&characterEncoding=UTF-8"
      val username = "root"
      val password = "Symbio@123"
      this.connection = DriverManager.getConnection(url, username, password)
      val sqlInsert = "insert into sensor_log(window,country,temperature) values(?,?,?)"
      psInsert = this.connection.prepareStatement(sqlInsert)
      val sqlUpdate = "update sensor_log set temperature=? where window=?,country=?"
      psUpdate = this.connection.prepareStatement(sqlUpdate)
      println("mysql建立连接")
    }

    override def invoke(value: sourceTemperature): Unit = {
      //如果没有查到数据，就插入
      if (psUpdate.getUpdateCount == 0) {
        psInsert.setString(1, value.country)
        psInsert.setString(2, value.temprature.toString)
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





  //    class enhanceMysqlSinkFunc extends RichSinkFunction[sourceTemperature] with CheckpointedFunction {
  //      //存储算子的状态值
  //      private  var pendingCasesPerCheckpoint :MapState[Long,util.ArrayList[String]] =_
  //
  //      override def close(): Unit = super.close()
  //
  //      override def open(parameters: Configuration): Unit = super.open(parameters)
  //
  //      override def invoke(value: sourceTemperature, context: SinkFunction.Context[_]): Unit = super.invoke(value, context)
  //
  //      override def snapshotState(context: FunctionSnapshotContext): Unit = {
  //        val checkpointId = context.getCheckpointId
  //        import java.util
  //        var cases = pendingCasesPerCheckpoint.get(checkpointId)
  //        if (cases == null) {
  //          cases = new util.ArrayList[String]
  //          pendingCasesPerCheckpoint.put(checkpointId, cases)
  //        }
  //        cases.addAll(pendingCases)
  //      }
  //
  //      override def initializeState(context: FunctionInitializationContext): Unit = ???
  //    }


  /**
   * 定义waterMark
   */
  class TimeExtractor extends AssignerWithPeriodicWatermarks[sourceTemperature] {

    val maxOufOfOrderness = 10000 //最大乱序时间 10s
    var currentMaxEventTime = 0L //设置当前窗口里面最大的时间

    /**
     * 计算watermark
     *
     * @return
     */
    override def getCurrentWatermark: Watermark = {
      new Watermark(currentMaxEventTime - maxOufOfOrderness)
    }

    /**
     * 指定我们的时间字段
     *
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

  class MyCountTrigger extends Trigger[sourceTemperature ,TimeWindow]{
    override def onElement(element: sourceTemperature, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
  }


}
