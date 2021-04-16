package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.beau.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

object DauApp {
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    // TODO: 2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    // TODO: 消费kafka中得数据
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)
    /*   kafkaStream.foreachRDD(
            rdd => {
              rdd.foreach(record => println(record.value()))
            }
          )*/
    // TODO:4.将JSON格式的数据转为样例类，并补全logDate和logHour
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaStream.mapPartitions(partition => {
      partition.map(
        record => {
          //    a. 将数据转为样例类
          val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
          //     b. 补全logDate需要对时间戳做格式化
          //      yyyy-MM-dd HH
          val times: String = sdf.format(new Date(startUpLog.ts))
          startUpLog.LogDate = times.split(" ")(0)
          startUpLog.LogHour = times.split(" ")(1)
          startUpLog
        }
      )
    })
    // TODO: 因为多次使用，所以加个缓存
    startUpLogDStream.cache()
    // TODO: 5. 跨批次去重
    val filterByRedisDSsteam: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream, ssc.sparkContext)
    //todo 原始数据条数
    startUpLogDStream.count().print()
    // todo 经过跨批次去重后得到的数据条数
    filterByRedisDSsteam.count().print()
    // TODO: 6.批次内去重
    val filterByMidDStream: DStream[StartUpLog] = DauHandler.filterByMid(filterByRedisDSsteam)
//  经过批次内去重后的一个数据条数
    filterByMidDStream.cache()
    filterByMidDStream.count().print()
//  TODO 7. 将去重后的结果mid写入redis，方便下个批次的数据去重
    DauHandler.saveMidToRedis(filterByMidDStream)
    // TODO: 将去重后的数据写入HBASE
    filterByMidDStream.foreachRDD(rdd=>{
     rdd.saveToPhoenix(
       "GMALL2021_DAU",
       Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
       HBaseConfiguration.create,
       Some("hadoop102,hadoop103,hadoop104:2181")
     )
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
