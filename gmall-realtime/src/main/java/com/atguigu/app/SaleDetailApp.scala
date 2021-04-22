package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.beau.{OrderDetail, OrderInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, SaleDetail}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming.CommitMetadata.format
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis
import java.util
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable.ListBuffer

/**
 * =>网络延迟所带来的的数据丢失问题|
 *
 *  - 灵活分析需求
 *    - 实时模块的作用:将order_info order_detail user_info这三张表的数据关联起来,然后明细数据保存至ES
 *    - 重点解决网络延迟带来的数据丢失问题
 *
 *
 *
 */
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    // TODO: 1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
    // TODO: 2. 创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    // TODO: 3. 获取Kafka中GMALL_ORDER_TOPIC中的数据
    val detailDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)
    // TODO: 获取Kafka中GMALL_ORDER_DETAIL_TOPIC中的数据
    val orderInfoDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)
    // TODO: 将数据转换为样例类,并将数据转为KV形式,为了后面的join使用
    val idToinfoDStream: DStream[(String, OrderInfo)] = orderInfoDStream.mapPartitions(_.map(
      record => {
        // TODO: 将数据转为样例类
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        // TODO: 补全 date hour 字段 
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        (orderInfo.id, orderInfo)
      }
    ))
    // TODO: 将order_detail转为样例类 
    val idToDetailDStream: DStream[(String, OrderDetail)] = detailDStream.mapPartitions(_.map(record => {
      // TODO: 将数据转为样例类 
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      // TODO: 转为kv键值对 
      (orderDetail.order_id, orderDetail)
    }))
    // TODO: 双流JOIN
    /**
     * => 双流join的解决办法
     *
     * - order_info 和 order_detail 的关系是1对多
     * - 对于order_info来说,无论如何 它都需要写到redis缓存中，因为不确定是否还有可以和它关联的order_detail数据因为网络延迟问题还没到达
     * - 对于order_detail来说,只要它被order_info关联了，就不需要缓存自己了;也就是说，只要当它在本批次没有被order_info关联，才需要写入redis的
     * 缓存中
     * => 解决思路
     *
     *    - 对于order_info而言
     *      - 首先将自己写入redis缓存
     *      - 其次查询本批次中是否有能和自己关联的order_detail数据
     *      - 之后去order_detail的缓存中查询，是否有能和自己关联的数据
     *
     *    - 对于order_detail而言
     *      - 首先在本次中查询有没有能和自己关联的order_info
     *      - 如果上一步中没能关联，就去对方的redis缓存中查询是否有和自己关联的数据
     *      - 如果都没有，就将自己写入redis缓存;只要它被关联了，就不需要缓存自己
     *
     *    - 采用 full outer jon
     *      - 目的是为了能获取关联不上的数据，将它保存到redis缓存
     *
     *
     */
    // TODO: 双流JOIN - full outer join 将两个流join起来
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToinfoDStream.fullOuterJoin(idToDetailDStream)
    fullJoinDStream.mapPartitions(
      partitions => {
        // TODO: 1. 创建结果结合，用来保存能够关联上的数据
        // TODO: 在一次匹配中,可能会产生多条匹配成功的结果，把每次匹配成功的结果放入可变的容器中
        val details: ListBuffer[SaleDetail] = new ListBuffer[SaleDetail]
        val jedis = new Jedis()
        // TODO: 2. 操作数据
        partitions.foreach { case (orderId, (infoOpt, detailOpt)) => {
          // TODO: 3. 判断order_info数据是否存在
          if (infoOpt.isDefined) {
            //      Order_info存在,获取orderinfo数据、
            val orderInfo: OrderInfo = infoOpt.get
            //            判断orderDetail数据是否存在
            if (detailOpt.isDefined) {
              val orderDetail: OrderDetail = detailOpt.get
              details += new SaleDetail(orderInfo, orderDetail)
            }

            // TODO: order_info不为空，将它写入redis
            /**
             * => orderInfo
             *
             *  - 1.存什么
             *    - orderInfo
             *  - 2. 用什么类型
             *  - 3. redisKey怎么设计
             *    - “OrderInfo:”OredrID
             */
            val infoRedisKey: String = "orderInfo:" + orderId
            val detailRedisKey: String = "orderDetail:" + orderId
            // TODO: 将样例类转为JSON字符串
            implicit val formats = org.json4s.DefaultJsonFormats
            val orderInfoJson: String = Serialization.write(orderInfo)
            // TODO: 写入redis缓存
            jedis.set(infoRedisKey,orderInfoJson)
            jedis.expire(infoRedisKey,10)
            // TODO: 去orderDetail缓存中查询是否有能关联上的数据
            val orderDetails: util.Set[String] = jedis.smembers(detailRedisKey)
            for (elem <- orderDetails.asScala) {
              // TODO: 将查出来的orderDetail字符串转为样例类
              val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              details += new SaleDetail(orderInfo,orderDetail)
            }


          } else{
            // TODO: 主表为null,从表一定不为null 
          }

        }
        }
        jedis.close()
        partitions
      }


    )


    ssc.start()
    ssc.awaitTermination()

  }

}
