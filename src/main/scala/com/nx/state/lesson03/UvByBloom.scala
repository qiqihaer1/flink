package com.nx.state.lesson03

import java.sql.Timestamp


import com.nx.state.utils.Utils

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 *   redis-server.exe redis.windows.conf
 *
 *   redis-cli.exe -h 127.0.0.1 -p 6379
 *
 *   keys *
 *
 *   HGETALL runoobkey
 *
 *   flushall
 *
  */
object UvByBloom {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据
    env.readTextFile(Utils.userBehaviorLogPath)
      .map(Utils.string2UserBehavior(_)) //解析数据
      .assignTimestampsAndWatermarks(new EventTimeExtractor) //添加水位
      .filter(_.behavior == "P") // 只统计pv操作
      .map(data => ("key", data.userId)) //把数据变为key,value结构
      .keyBy(_._1) //按照key进行分组（注意：如果数据量特别大，添加一个随机的前缀进行处理）
      .timeWindow(Time.hours(1)) //窗口是1小时
      .trigger(new MyTrigger()) //每来来一条数据都会更新一下结果
      .process(new UvCountWithBloom())
      .print()


    env.execute("UvByBloom")
  }
}

// 自定义窗口触发器
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // 每来一条数据，就直接触发窗口操作，并清空所有窗口状态
    TriggerResult.FIRE_AND_PURGE
  }
}

// 定义一个布隆过滤器
class Bloom(size: Long) extends Serializable {
    //布隆过滤器的默认大小是32M
    //32 * 1024 * 1024 * 8
    //2^5  2^10   2^10 * 2^3
  
  //1后面28个0
  private val cap = if (size > 0) size else 1 << 28

  //定义hash函数的结果，当做位图的offset
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for( i <- 0 until value.length ){
      //各种方法去实现都行
      result += result * seed + value.charAt(i)
    }
    //他们之间进行&运算结果一定在位图之间
    result  & ( cap - 1 ) //0后面28个1
  }
}

class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), Tuple2[String,Long], String, TimeWindow]{
  // 定义redis连接
  lazy val jedis = new Jedis("localhost", 6379)

  //32 * 1024 * 1024 * 8
  //5 + 10 + 10 + 3
  lazy val bloom = new Bloom(1<<28)

  override def process(key: String, context: Context,
      elements: Iterable[(String, Long)], out: Collector[Tuple2[String,Long]]): Unit = {
    // 位图的存储方式，key是windowEnd，value是bitmap
    //val storeKey = context.window.getEnd.toString
    val storeKey =new Timestamp(context.window.getEnd).toString
    
    var count = 0L
    // 把每个窗口的uv count值也存入名为count的redis表，
    // 存放内容为（windowEnd -> uvCount），所以要先从redis中读取
    if( jedis.hget("count", storeKey) != null ){
      //UV取出来
      count = jedis.hget("count", storeKey).toLong
    }
    // 用布隆过滤器判断当前用户是否已经存在
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)
    // 定义一个标识位，判断reids位图中有没有这一位
    val isExist = jedis.getbit(storeKey, offset)
    if(!isExist){
      // 如果不存在，位图对应位置1，count + 1
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect( Tuple2[String,Long](new Timestamp(context.window.getEnd).toString, count + 1) )
    } else {
      out.collect( Tuple2[String,Long](new Timestamp(context.window.getEnd).toString, count) )
    }
  }
}

