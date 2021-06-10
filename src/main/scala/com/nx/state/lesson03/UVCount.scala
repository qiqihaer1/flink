package com.nx.state.lesson03

import java.lang
import java.sql.Timestamp


import com.nx.state.utils.Utils
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector



//用户行为对象
case class UserBehavior(
                         userId:Long,
                         productId:Long,
                         categoryId:Long,
                         behavior:String,
                         timeStamp:Long,
                         sessionId:String)

object UVCount {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置参数
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    env.readTextFile(Utils.userBehaviorLogPath) //读取数据
      .map(Utils.string2UserBehavior(_)) //解析数据
      .assignTimestampsAndWatermarks(new EventTimeExtractor) //添加水位
      .filter(_.behavior == "P") //过滤用户行为
      .timeWindowAll(Time.hours(1)) //滚动的窗口
      .apply(new UvCountByWindow) //统计UV
      .print()

    env.execute("uv count....")
  }
}

class UvCountByWindow() extends
  AllWindowFunction[UserBehavior,Tuple2[String,Long],TimeWindow]{
  override def apply(window: TimeWindow,
                     values: Iterable[UserBehavior],
                     out: Collector[Tuple2[String,Long]]): Unit = {
    //定一个SET集合，天然的实现去重的效果
    var userIdSet = Set[Long]()

    for(user <- values){
      userIdSet += user.userId
    }
    //如果我们想要看有多少UV，那么我们直接看这个集合的大小就行了。
    out.collect(Tuple2(new Timestamp(window.getEnd).toString,userIdSet.size))
  }


}

/**
 * 指定时间字段
 * 指定延迟时间
 */
class EventTimeExtractor extends AssignerWithPeriodicWatermarks[UserBehavior]{
  var currentMaxEventTime = 0L;
  val maxOutOfOrderness = 10;//10 000
  override def getCurrentWatermark: Watermark = {
    new Watermark((currentMaxEventTime - maxOutOfOrderness) * 1000)
  }

  override def extractTimestamp(userBehavior: UserBehavior, l: Long): Long = {
    val timeStamp = userBehavior.timeStamp * 1000
    currentMaxEventTime = Math.max(timeStamp,currentMaxEventTime)
    timeStamp
  }
}
