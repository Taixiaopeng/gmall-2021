package com.atguigu.handler

import com.atguigu.beau.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util
import java.util.Date

object DauHandler {
  /**
   * 批次内去重
   *
   * @param filterByRedisDStream
   */
  def filterByMid(filterByRedisDStream: DStream[StartUpLog]) = {
    // TODO: 1. 将数据转为K V ((mid,logdate),StartUpLog)
    val midAndDateToLogDSteam: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(
      log => {

        ((log.mid, log.LogDate), log)
      }
    )
    // TODO: 2. 使用groupbykey将相同的key的数据聚合在一起
    val midAndDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndDateToLogDSteam.groupByKey()
    // TODO: 对相同的key的数据按照时间排序，并取出第一条
    val sortWithTsDStream: DStream[((String, String), List[StartUpLog])] = midAndDateToLogIterDStream.mapValues(
      iter => {
        iter.toList.sortWith(_.ts < _.ts).take(1)
      }
    )
    // TODO: 将value的list集合打撒并返回
    val value: DStream[StartUpLog] = sortWithTsDStream.flatMap(_._2)
    value


  }

  /**
   * 跨批次去重
   *
   * @param startUpLogDStream
   * @param sc
   * @return
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {
    /*    // TODO: 对数据进行去重操作 【方案一】
        val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
          //      获取Redis链接
          val jedisClient: Jedis = RedisUtil.getJedisClient
          //      rediskey
          val rediskey = "DAU:" + log.LogDate
          //      拿本批次的mid去redis中对比数据，判断是否存在
          val boolean: lang.Boolean = jedisClient.sismember(rediskey, log.mid)
          //      关闭连接
          jedisClient.close()
          !boolean
        })
        value*/

    // TODO: 方案二 在分区下获取连接以减少连接次数
    /*    startUpLogDStream.mapPartitions(partition => {
          //      a. 获取redsis连接
          val jedisClient: Jedis = RedisUtil.getJedisClient
          println("123")
          val logs: Iterator[StartUpLog] = partition.filter(log => {
            //        b. rediskey
            val redisKey: String = "DAU: " + log.LogDate
            //        c. 拿本批次的mid去redis中对比数据，判断是否存在
            val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)
            !boolean
          })

          //      关闭连接
          jedisClient.close()

          logs

        })*/
    // TODO: 方案三 在每个批次内获取一次连接
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value3: DStream[StartUpLog] = startUpLogDStream.transform(
      rdd => {
        //        1. 获取redis连接(driver端)
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //        2. 去redis中获取数据(driver端)

        val redisKey: String = sdf.format(new Date(System.currentTimeMillis()))
        val mid: util.Set[String] = jedisClient.smembers(redisKey)
        //        3. 将driver获取的redis中的数据利用广播变量将其广播到excutor端
        val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mid)
        val rddBC: RDD[StartUpLog] = rdd.filter(log => { //todo 执行位置在executor
          !midsBC.value.contains(log.mid)
        })
        // 4.关闭连接
        jedisClient.close()
        rddBC
      }
    )


    value3
  }


  // TODO: 将去重后的结果写入Redis，方便下个批次去重
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(
        partition => {
          // TODO: 获取redis连接
          val jedisClient: Jedis = RedisUtil.getJedisClient
          // TODO: 在foreach算子中将数据写入redis
          partition.foreach(log => {
            //rediskey
            val rediskey: String = "DAU: " + log.LogDate
            // 根据key将数据保存至redis
            jedisClient.sadd(rediskey,log.mid)

          })
          jedisClient.close()
        }
      )
    })
  }
}
