package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.beau.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //    kafkaDStream.foreachRDD(rdd=>{
    //      rdd.foreach(record=>{
    //        println(record.value())
    //      })
    //    })

    //4.将数据转化为样例类 EventLog,因为下游要按照mid分组，所以将数据以kv的形式返回
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //将json数据转为样例类
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

        //补充字段
        eventLog.logDate = sdf.format(new Date(eventLog.ts)).split(" ")(0)
        eventLog.logHour = sdf.format(new Date(eventLog.ts)).split(" ")(1)

        (eventLog.mid, eventLog)
      })
    })

    //5.开窗，5min
    val midToLogWindowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    //6.将相同mid的数据聚和到一块
    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = midToLogWindowDStream.groupByKey()

    //7.筛选数据=>产生疑似预警日志
    val boolToCouponDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogIterDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) => {

        //7.1创建set集合（java）用来保存涉及的用户id->uid
        val uids: util.HashSet[String] = new util.HashSet[String]()
        //创建领优惠券涉及的商品id集合
        val itemIds: util.HashSet[String] = new util.HashSet[String]()
        //创建用户行为事件的集合
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        //7.2遍历数据
        //定义标志位,用来判断是有浏览商品行为
        var bool: Boolean = true

        breakable {
          iter.toList.foreach(log => {
            events.add(log.evid)
            if ("clickItem".equals(log.evid)) {
              bool = false
              break()
            } else if ("coupon".equals(log.evid)) {
              uids.add(log.uid)
              itemIds.add(log.itemid)
            }
          })
        }

        //7.3生成疑似预警日志
        ((uids.size() >= 3 && bool), CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
      }
    })
      boolToCouponDStream.print(5000)

    //8.生成预警日志
    val couponAlertDStream: DStream[CouponAlertInfo] = boolToCouponDStream.filter(_._1).map(_._2)

    couponAlertDStream.print()

    //9.将预警日志写入ES 保证同一设备，每分钟记录一次预警
    couponAlertDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val list: List[(String, CouponAlertInfo)] = partition.toList.map(coupon => {
          (coupon.mid + coupon.ts / 1000 / 60, coupon)
        })

        //利用ES工具类将数据写入ES k->索引名 v->list[docId,具体数据]
        MyEsUtil.insertBulk(GmallConstants.ES_INDEX_ALERT+sdf.format(new Date(System.currentTimeMillis())).split(" ")(0),list)
      })
    })

    //10.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
