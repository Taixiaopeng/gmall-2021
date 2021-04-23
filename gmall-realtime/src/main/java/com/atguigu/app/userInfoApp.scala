package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.beau.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import redis.clients.jedis.Jedis

object userInfoApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoApp")
    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
    //3.获取Kafka中GMALL_ORDER-topic中数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER,ssc)
    kafkaDStream.map(record=>record.value()).print()
    //4.将userinfo数据写入redis
    kafkaDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val jedis = new Jedis("hadoop102", 6379)
        partition.foreach(record=>{
          //        为了拿到userId,因此将数据转为样例类
          val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
          val userInfoRedisKey = "userInfo:"+userInfo.id
          jedis.set(userInfoRedisKey,record.value())
        }
    )
        jedis.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()


  }

}
