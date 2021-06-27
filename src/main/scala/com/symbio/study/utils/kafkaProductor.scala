package com.symbio.study.utils

import java.text.SimpleDateFormat
import java.util.{Date, Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object kafkaProductor {

  def main(args: Array[String]): Unit = {
    val prop = new Properties
    //指定kafka broker地址
    prop.put("bootstrap.servers", "vmnode01:9092")
    //指定key value的序列化方式
    prop.put("key.serializer", classOf[StringSerializer].getName)
    prop.put("value.serializer", classOf[StringSerializer].getName)
    //指定topic名称
    val topic = "from_flume01"

    //创建producer链接
    val producer = new KafkaProducer[String, String](prop)

    //producer发出消息
    while (true) {
      val message = "{\"dt\":\"" + getCurrentTime + "\",\"countryCode\":\"" + getCountryCode + "\",\"site\":\""+getRandomType+"\",\"temperature\":\""+getRandomTemperatureType+"\"}"
      //同步的方式，往Kafka里面生产数据
      producer.send(new ProducerRecord[String, String](topic, message))
      System.out.println(message)
      Thread.sleep(30000)
    }
    //关闭链接
    //        producer.close();


  }


  def getCurrentTime: String = {
    val format ="YYYY-MM-dd HH:mm:ss"
    val sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
    sdf.format(new Date)
//    val time: LocalDateTime = new LocalDateTime()
//    val str: String = DateTimeFormatter.ofPattern(format).format(time)
//    str
  }

  def getCountryCode: String = {
    val types = Array("US", "TW", "HK", "PK", "KW", "SA", "IN")
    val random = new Random
    val i = random.nextInt(types.length)
    types(i)
  }


  def getRandomType: String = {
    val types = Array("s1", "s2", "s3", "s4", "s5")
    val random = new Random
    val i = random.nextInt(types.length)
    types(i)
  }

  def getRandomTemperatureType: String = {
    val random = new Random
    val i = 37d +random.nextGaussian().toString.substring(0,4).toDouble
    i.toString
  }


}
