package com.nx.state.utils

import java.text.SimpleDateFormat

import com.nx.state.lesson01.{ApacheLogEvent, LogEvent}
import com.nx.state.lesson02.AdClickEvent
import com.nx.state.lesson03.UserBehavior

object Utils {

  //时间日志路径
  val eventLogPath = "D:\\yingxu.liao\\nx-flink-state\\src\\main\\resources\\data2.log"
  //广告点击日志路径
  val adClickLogPath = "D:\\yingxu.liao\\nx-flink-state\\src\\main\\resources\\data3.csv"

  //用户行为数据日志路径
  val userBehaviorLogPath="D:\\yingxu.liao\\nx-flink-state\\src\\main\\resources\\data1.csv"


  /**
   * 根据字符串把数据转换成为日志服务数据对象
   * @param line
   * @return
   */
  def string2ApacheLogEvent(line:String):ApacheLogEvent={
    val fields = line.split(" ")
    val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
    val timeStamp = dateFormat.parse(fields(3).trim).getTime
    ApacheLogEvent(fields(0).trim,fields(1).trim,timeStamp,
      fields(5).trim,fields(6).trim)
  }

  /**
   * 根据字符串生成广告点击日志对象
   * @param line
   * @return
   */
  def string2ClickEvent(line:String):AdClickEvent={
    val dataArray = line.split(",")
    AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
  }

  /**
   * 根据字符串，把数据转换成为用户行为对象
   * @param line
   * @return
   */
  def string2UserBehavior(line:String):UserBehavior={
    val fields = line.split(",")
    UserBehavior(fields(0).trim.toLong,
      fields(1).trim.toLong,
      fields(2).trim.toLong,
      fields(3).trim,
      fields(4).trim.toLong,
      fields(5).trim
    )

  }

}
