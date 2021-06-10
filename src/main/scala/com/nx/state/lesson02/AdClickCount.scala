package com.nx.state.lesson02

import java.sql.Timestamp

import com.nx.state.utils.Utils
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// 输入的广告点击事件样例类
case class AdClickEvent( userId: Long,  //用户ID
                         adId: Long, //广告ID
                         province: String,  //省份
                         city: String, //城市
                         timestamp: Long ) //用户点击广告的时间

/**
 * 实时黑名单统计
 */
object AdClickCount {
  //设置了一个侧输出流 外部的变量，全局的变量
  private val outputBlackList = new OutputTag[String]("blacklist")


  def main(args: Array[String]): Unit = {
    //步骤一：获取程序入口
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置参数
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //步骤二：计算黑名单
    val adEventStream = env.readTextFile(Utils.adClickLogPath) //获取数据
      .map(Utils.string2ClickEvent(_)) //解析数据
      .assignTimestampsAndWatermarks(new AdClickEventTimeExtractor()) //设置watermark
      .keyBy(data => (data.userId, data.adId)) //分组(userid,adclickid)
      .process(new CountBlackListUser(100)) //实时统计

    //步骤三：从侧输出流打印黑名单
    adEventStream.getSideOutput(outputBlackList)
        .print()



    env.execute("AdClickCount")

  }


//  /**
//   * 对广告点击次数进行聚合统计
//   */
//  class AdClickCount extends AggregateFunction[AdClickEvent,Long,Long]{
//    //辅助变量赋初始值
//    override def createAccumulator(): Long = 0L
//    //对每条数据加一
//    override def add(in: AdClickEvent, acc: Long): Long = acc + 1
//    //返回最后的结果
//    override def getResult(acc: Long): Long = acc
//    //把所有的数据加起来
//    override def merge(acc: Long, acc1: Long): Long = acc + acc1
//  }

  /**
   * 过滤黑名单数据
   * @param maxCount 最大次数
   */
  class CountBlackListUser(maxCount:Int)
    extends KeyedProcessFunction[(Long,Long),AdClickEvent,AdClickEvent]{
    //记录当前用户对当前广告的点击量
    lazy val clickCountState:ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("count-click-state",classOf[Long]))
    //保存是否发送过黑名单
    lazy val isSetBlackList:ValueState[Boolean] = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("is-sent-state",classOf[Boolean]))
    //保存定时器触发的时间戳
    lazy val saveTimerState:ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("reset-time-state",classOf[Long]))

    override def processElement(value: AdClickEvent,
                                ctx: KeyedProcessFunction[(Long, Long),
                                  AdClickEvent, AdClickEvent]#Context,
                                out: Collector[AdClickEvent]): Unit = {

      val currentCount = clickCountState.value()
      //如果当前用户的当前广告第一次来，注册定时器，定时器每天00:00触发
      //也就是说，到了晚上12：00的时候，你要清空今天统计的数据。
      if(currentCount == 0){
       //计算时间
       val ts = (ctx.timerService().currentProcessingTime()/(1000*60*60*24) +1) * (1000 * 60 * 60 * 24)
        saveTimerState.update(ts)
        //注册定时器
        ctx.timerService().registerProcessingTimeTimer(ts)
      }
      //判断计数是否达到上线，如果达到加入黑名单
      if(currentCount >= maxCount){ //100 101
        //是否发送过黑名单
        if(!isSetBlackList.value()){ //如果没有发送过黑名单消息
          //更新一下发送黑名单的状态
          isSetBlackList.update(true)
         //输入到侧输出流
          ctx.output(outputBlackList,
            "用户"+value.userId+" 对广告："+value.adId+" 点击超过 " + maxCount +" 次")
        }
        return
      }
      //更新当前的状态，累加访问的次数
      clickCountState.update(currentCount + 1)
      out.collect(value)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[(Long, Long),
                           AdClickEvent, AdClickEvent]#OnTimerContext,
                           out: Collector[AdClickEvent]): Unit = {
      if(timestamp == saveTimerState.value()){
        //清空状态数据
        isSetBlackList.clear()
        clickCountState.clear()
        saveTimerState.clear()
      }

    }

  }

}

class AdClickEventTimeExtractor extends AssignerWithPeriodicWatermarks[AdClickEvent]{
   //当前窗口的时间最大值
  var currentMaxEventTime = 0L
  //最大乱序时间 10s
  val maxOufOfOrderness = 10

  override def getCurrentWatermark: Watermark = {
    new Watermark((currentMaxEventTime - maxOufOfOrderness) * 1000)
  }

  override def extractTimestamp(element: AdClickEvent, previousElementTimestamp: Long): Long = {
    //时间字段
    val timestamp = element.timestamp * 1000

    currentMaxEventTime = Math.max(element.timestamp, currentMaxEventTime)
    timestamp;
  }
}
