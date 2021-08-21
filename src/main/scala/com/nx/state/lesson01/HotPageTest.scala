package com.nx.state.lesson01

import java.text.SimpleDateFormat

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


//输入数据样例类
case class LogEvent(ip: String, //IP地址
                    userId: String, //用户ID
                    eventTime: Long, //用户点击广告时间
                    method: String, //请求方式
                    url: String) //请求的URL


// 窗口聚合结果样例类
case class UrlCount(url: String, //请求的URL
                    windowEnd: Long, //所属窗口
                    count: Long) //点击的次数


/**
 * 热门页面统计
 */
object HotPageTest {

  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val lateTag = new OutputTag[LogEvent]("late-data")

    val stream = env.readTextFile("D:\\yingxu.liao\\nx-flink-state\\src\\main\\resources\\data2.log")
      .map(string2LogEvent(_)) //对数据进行解析
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LogEvent](Time.seconds(2)) {
        override def extractTimestamp(element: LogEvent): Long = element.eventTime * 1000
      }) //设置水位，允许数据迟到10秒
      .keyBy(_.url)
      .timeWindow(Time.seconds(60), Time.seconds(5)) //设置窗口
      .allowedLateness(Time.seconds(10))
      .sideOutputLateData(lateTag)
      .process(new MyKeyedProcessFunction(lateTag))


    stream.print("normal")

    stream.getSideOutput(lateTag).print("late-data")

    env.execute("hot page count")
  }

  def string2LogEvent(line: String): LogEvent = {
    val fields = line.split(" ")
    val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
    val timeStamp = dateFormat.parse(fields(3).trim).getTime
    LogEvent(fields(0).trim, fields(1).trim, timeStamp,
      fields(5).trim, fields(6).trim)
  }

}


  class MyKeyedProcessFunction(lateTag : OutputTag[LogEvent]) extends ProcessWindowFunction[LogEvent,UrlCount,String,TimeWindow]{

    var pvState: ValueState[Int] = _
    var uvState: ValueState[Int] = _
    var startTimeState: ValueState[Long] = _


    override def open(parameters: Configuration): Unit = {

      pvState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("pv",classOf[Int]))
      uvState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("pv",classOf[Int]))

    }

    override def process(key: String, context: Context, elements: Iterable[LogEvent], out: Collector[UrlCount]): Unit = {
      val value = UrlCount(elements.iterator.next().url, context.window.getEnd, pvState.value())

//      context.output(lateTag,UrlCount(elements.iterator.next().url,context.window.getEnd,pvState.value()))


      out.collect(value)

    }
  }

// class MyKeyedProcessFunction(i:Int) extends KeyedProcessFunction[String,LogEvent,String]{
//
//   var myValueState: ValueState[Int] = _
//
//   override def open(parameters: Configuration): Unit = {
//
//     myValueState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("count",classOf[Int]))
//   }
//
//   override def processElement(value: LogEvent, ctx: KeyedProcessFunction[String, LogEvent, String]#Context, out: Collector[String]): Unit = {
//     val url = ctx.getCurrentKey
//     val timestamp = ctx.timestamp()
//     val watermark = ctx.timerService().currentWatermark()
//
//     //
//     ctx.output()
//
//     val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
//
//     val currentCount = myValueState.value() + 1
//
//     myValueState.update(currentCount)
//
//     if (currentCount>9){
//
//       ctx.timerService().registerEventTimeTimer(timestamp + 60000L)//60S
//     }
//
//   }
//
//
//   override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, LogEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
//
//   }
//
//
// }














