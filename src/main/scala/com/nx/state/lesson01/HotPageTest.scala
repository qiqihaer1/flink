//package com.nx.state.lesson01
//
//import java.sql.Timestamp
//import java.text.SimpleDateFormat
//
//import com.nx.state.utils.Utils
//import org.apache.flink.api.common.functions.AggregateFunction
//import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.scala.function.WindowFunction
//import org.apache.flink.streaming.api.watermark.Watermark
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//
//import scala.collection.mutable.ListBuffer
//
//
////输入数据样例类
//case class LogEvent(ip: String, //IP地址
//                    userId: String, //用户ID
//                    eventTime: Long, //用户点击广告时间
//                    method: String, //请求方式
//                    url: String) //请求的URL
//
//
//
//// 窗口聚合结果样例类
//case class UrlCount(url: String, //请求的URL
//                        windowEnd: Long, //所属窗口
//                        count: Long) //点击的次数
//
//
///**
// * 热门页面统计
// */
//object HotPageTest {
//
//  def string2LogEvent(line:String):LogEvent={
//    val fields = line.split(" ")
//    val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
//    val timeStamp = dateFormat.parse(fields(3).trim).getTime
//    LogEvent(fields(0).trim,fields(1).trim,timeStamp,
//      fields(5).trim,fields(6).trim)
//  }
//
//
//  def main(args: Array[String]): Unit = {
//    //获取执行环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    //设置时间
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)
//
//    env.readTextFile("D:\\yingxu.liao\\nx-flink-state\\src\\main\\resources\\data2.log") //读取到数据
//      .map(string2LogEvent(_)) //使用面向对象的思想，对数据进行解析
//      .assignTimestampsAndWatermarks(new HotPageTestEventTimeExtractor) //设置水位，允许数据迟到10秒
//        .keyBy(_.url)
//        .timeWindow(Time.seconds(60),Time.seconds(5)) //设置窗口
//        .flatMap()
//
//    env.execute("hot page count")
//  }
//
//}
//
//
//class TopNHotPageTest(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
//  //申明一个state，里面存储URL和对应出现的次数
//  //TODO 这个地方用ListState也可以
//  lazy val urlState: MapState[String, Long] =
//  getRuntimeContext.getMapState(new MapStateDescriptor[String, Long](
//    "url-state-count", classOf[String], classOf[Long]))
//
//  override def processElement(value: UrlViewCount,
//                              ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
//                              out: Collector[String]): Unit = {
//    //来一条数据就把数据给存起来
//    urlState.put(value.url, value.count)
//    //注册定时器
//    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
//  }
//
//  override def onTimer(timestamp: Long,
//                       ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
//                       out: Collector[String]): Unit = {
//    //里面可以实现排序
//    val allUrlViews: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]()
//
//    val iter = urlState.entries().iterator()
//    while (iter.hasNext) {
//      val entry = iter.next()
//      allUrlViews += ((entry.getKey, entry.getValue))
//    }
//    //清空state
//    urlState.clear()
//    //使用降序排序，求TopN
//    val sortedUrlView = allUrlViews.sortWith(_._2 > _._2).take(topSize)
//
//    val result = new StringBuilder()
//    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
//    sortedUrlView.foreach(view => {
//      result.append("URL:").append(view._1)
//        .append(" 访问量：").append(view._2).append("\n")
//    })
//    result.append("===================")
//
//    out.collect(result.toString())
//  }
//}
//
///**
// * 自定义窗口处理函数
// */
//class PageWindow extends WindowFunction[Long, UrlViewCount, String, TimeWindow]{
//  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit =
//    out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
//}
////class PageWindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
////  override def apply(key: String, window: TimeWindow,
////                     input: Iterable[Long],
////                     out: Collector[UrlViewCount]): Unit = {
////    //window.getEnd 标示我们的一个窗口
////    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
////  }
////}
//
///**
// * 实现的是对URL进行聚合
// * sum
// * 辅助变量，累加变量
// */
//class eventCountSum extends AggregateFunction[LogEvent, Long, Long] {
//
//  override def createAccumulator(): Long = 0L
//
//  override def add(value: LogEvent, accumulator: Long): Long = accumulator + 1
//
//  override def getResult(accumulator: Long): Long = accumulator
//
//  override def merge(a: Long, b: Long): Long = a + b
//}
//
//
/////**
//// * 定义waterMark
//// */
////class HotPageEventTimeExtractor extends AssignerWithPeriodicWatermarks[LogEvent] {
////
////  val maxOufOfOrderness = 10000 //最大乱序时间 10s
////  var currentMaxEventTime = 0L //设置当前窗口里面最大的时间
////
////  /**
////   * 计算watermark
////   *
////   * @return
////   */
////  override def getCurrentWatermark: Watermark = {
////    new Watermark(currentMaxEventTime - maxOufOfOrderness)
////  }
////
////  /**
////   * 指定我们的时间字段
////   *
////   * @param element
////   * @param previousElementTimestamp
////   * @return
////   */
////  override def extractTimestamp(element: LogEvent, previousElementTimestamp: Long): Long = {
////    //时间字段
////    val timestamp = element.eventTime
////    currentMaxEventTime = Math.max(element.eventTime, currentMaxEventTime)
////    timestamp;
////  }
////}
//
//
//
//
//class HotPageTestEventTimeExtractor extends AssignerWithPeriodicWatermarks[LogEvent]{
//  var currentMaxEventTime = 0L
//  val delayTime = 5000//5s
//
//
//  /**
//   * 指定我们的时间字段
//   * @param element
//   * @param previousElementTimestamp
//   * @return
//   */
//  override def extractTimestamp(element: LogEvent, previousElementTimestamp: Long): Long = {
//    val timeStamp = element.eventTime
//    currentMaxEventTime = Math.max(element.eventTime,currentMaxEventTime)
//    timeStamp
//  }
//
//  override def getCurrentWatermark: Watermark = {
//    new Watermark(currentMaxEventTime - delayTime)
//  }
//}
