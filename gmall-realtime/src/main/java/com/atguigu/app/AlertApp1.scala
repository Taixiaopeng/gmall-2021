package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.beau.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.util.control.Breaks.{break, breakable}


object AlertApp1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("AlterApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // TODO: 消费kafka的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)
    kafkaDStream.mapPartitions(_.map(_.value())).print()
    // TODO: 将数据转为样例类
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(_.map(record => {
      //todo 将json数据转为样例类
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

      eventLog.logDate = sdf.format(new Date(eventLog.ts)).split(" ")(0)
      eventLog.logHour = sdf.format(new Date(eventLog.ts)).split(" ")(1)
      (eventLog.mid, eventLog)
    }))

    // TODO: 5. 开窗 5min
    val midToWindowDSteam: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))
    // TODO: 6. 将相同mid的数据聚合到一起
    val midToIterDStream: DStream[(String, Iterable[EventLog])] = midToWindowDSteam.groupByKey()
    //============================筛选日志===============================================’
    val boolToCouponDstream: DStream[(Boolean, CouponAlertInfo)] = midToIterDStream.mapPartitions(_.map { case (mid, iter) => {
      // TODO:7.1 创建Set集合来保存涉及的用户id ->uid
      //  创建Set集合来对uid进行去重
      val uids = new util.HashSet[String]()
      //todo 创建领取优惠券涉及的商品id集合
      val itemIds = new util.HashSet[String]()
      //todo 创建用户行为事件的集合
      val events = new util.ArrayList[String]()
      // TODO: 7.2 遍历数据
      //todo 定义标志位, 用来判断是有浏览商品行为
      var bool: Boolean = true
      breakable {
        iter.toList.foreach(
          log => {
            events.add(log.evid)
            // TODO: 如果有浏览商品的行为,则跳出循环
            if ("clickItem".equals(log.evid)) {
              bool = false
              break()
            } else if ("coupon".equals(log.evid)) {
              uids.add(log.uid)
              // TODO: 领取优惠券涉及商品的ID
              itemIds.add(log.itemid)
            }
          }
        )
      }
      // TODO: 7.3 生成疑似预警日志
      ((uids.size() >= 3 && bool), CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
    }
    }

    )

    //TODO 8.生成预警日志
    val couponAlterDstream: DStream[CouponAlertInfo] = boolToCouponDstream.filter(_._1).map(_._2)
    // TODO: 9 将预警日志写入ES 保证同一设备,每分钟记录一次预警 利用ES的幂等性去重
    couponAlterDstream.foreachRDD(rdd => {
      rdd.foreachPartition(parition => {
        val list: List[(String, CouponAlertInfo)] = parition.toList.map(coupon => {
          (coupon.mid + coupon.ts / 1000 / 60, coupon)
        })

        // TODO: 利用ES工具类将数据写入ES  k-索引名 v->list[docID,具体数据]

        MyEsUtil.insertBulk(GmallConstants.ES_INDEX_ALERT+sdf.format(new Date(System.currentTimeMillis())).split(" ")(0),list)

      }
      )
    })


    ssc.start()
    ssc.awaitTermination()

  }

}