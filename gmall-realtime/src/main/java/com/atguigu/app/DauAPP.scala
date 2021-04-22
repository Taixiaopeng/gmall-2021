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

object DauAPP {
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    // TODO: 2. 创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    // TODO: 3. 消费Kafka中数据
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)
    // TODO: 4.将json格式的数据转换为样例类，并补全LogDate和LogHour
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaStream.mapPartitions(
      partitions => {
        partitions.map(record => {
          // TODO: a. 将数据转为样例类
          val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
          // TODO: b. 补全LogDate和LogHour需要对时间戳做格式化
          //        yyyy-MM-dd HH
          val times: String = sdf.format(new Date(startUpLog.ts))
          // TODO: 补全logDate LogHour
          startUpLog.LogDate = times.split(" ")(0)
          startUpLog.LogHour = times.split(" ")(1)
          startUpLog
        })
      }

    )
    // TODO: ？
    startUpLogDStream.cache()

    // TODO: 5. 跨批次去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.fileterByRedis(startUpLogDStream, ssc.sparkContext)
    filterByRedisDStream.cache()
    // TODO: 原始数据的条数
    startUpLogDStream.count().print()
    // TODO: 去重后的数据条数
    filterByRedisDStream.count().print()
    // TODO: 6. 批次内去重
    val  filterByMidDStream: DStream[StartUpLog] = DauHandler.fileterByMid(filterByRedisDStream)
    filterByMidDStream.cache()
    filterByMidDStream.count().print()
    // TODO: 7. 将去重后的数据写入Redis ,方便下批次的数据去做去重
    DauHandler.saveMidToRedis(filterByMidDStream)
    // TODO: 8.将去重后的数据写入Hbase
    filterByMidDStream.foreachRDD(
      _.saveToPhoenix(
        "GMALL2021_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181")

      )
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
