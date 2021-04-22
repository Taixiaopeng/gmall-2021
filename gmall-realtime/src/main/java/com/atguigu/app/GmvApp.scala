package com.atguigu.app

import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object GmvApp {
  def main(args: Array[String]): Unit = {
    // TODO: 获取StreamingComtext
    val ssc = new StreamingContext(new SparkConf().setMaster("local[*]").setAppName("Gmv"), Seconds(5))
    // TODO: 2.获取Kafka中的GMALL_ORDER-topic中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)
    // TODO: 3. 将JSON数据转为样例类
    kafkaDStream.mapPartitions(_.map(record => {
      //todo 将数据转为样例类
    }


    ))
  }

}
