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
import scala.util.control.Breaks._

/**
 * => 预警需求
 *
 *  - 需求: 实时预警,是一种经常出现在实时计算中的业务类型，根据日志数据中系统报错异常,或者用于行为
 *    异常的检测，产生对对应的预警日志。预警日志通过图形化界面的展示，可以提醒监控方
 *  - 需求说明：同一设备，5分钟内三次及以上用不同的账号登录并领取优惠券,并且过程中没有浏览商品;达到以上要求则产生一条预警日志,并且同一设备,一分钟内只记录一次预警；
 *
 * ==========================预警需求======================================================
 *  - 同一设备,(mid groupByKey)
 *  - 5分钟内(开5分钟的窗口)-- 5分钟内多个批次union到一起 -- 对这个整体数据集groupBykey
 *  - 三次及以上不同的账号领取优惠券,(uid >=3 ? coupon?)
 *       - 需要对uid去重,同一个账号登录多次也只算一次
 *       - 去重 -> set -> set集合的大小来判断有几个不同的用户
 *  - 过程中没有浏览商品 (反向判断,是否浏览商品,浏览的话就不符合预警日志)
 *  - 达到以上要求则产生一条预警日志(生成预警日志)
 *  - 并且同一设备,每分钟只记录一次预警。(保存到ES,doc id-> mid+精确到分钟时间)
 *
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    //3.消费kafka数据

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)
    //    kafkaDStream.map(_.value()).print()
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
    // ================================= 筛选日志================================================================
    //7.筛选数据=>产生疑似预警日志
    val boolToCouponDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogIterDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) => {

        //7.1创建set集合（java）用来保存涉及的用户id->uid
        // TODO: 采用set集合对uid进行去重
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
            // TODO: 浏览商品 跳出循环
            if ("clickItem".equals(log.evid)) {
              bool = false
              break()
            } else if ("coupon".equals(log.evid)) {
              uids.add(log.uid)
              // TODO: 领取优惠券涉及商品的ID
              itemIds.add(log.itemid)
            }
          })
        }

        //7.3生成疑似预警日志
        ((uids.size() >= 3 && bool), CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
      }
    })
    //8.生成预警日志           只取产生预警信息的日志
    val couponAlertDStream: DStream[CouponAlertInfo] = boolToCouponDStream.filter(_._1).map(_._2)
    couponAlertDStream.print(5000)
    //9.将预警日志写入ES 保证同一设备，每分钟记录一次预警  利用ES的幂等性去重
    couponAlertDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val list: List[(String, CouponAlertInfo)] = partition.toList.map(coupon => {

          (coupon.mid + coupon.ts / 1000 / 60, coupon)

        })
        //利用ES工具类将数据写入ES k->索引名 v->list[docId,具体数据]
        MyEsUtil.insertBulk(GmallConstants.ES_INDEX_ALERT + sdf.format(new Date(System.currentTimeMillis())).split(" ")(0), list)
      })
    })

    //10.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
