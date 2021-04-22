package com.atguigu.handler

import com.atguigu.beau.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.lang

/**
 *
 */
object DauHandler {


  def fileterByMid(fiilterByRedisDStream:DStream[StartUpLog]) ={
    // TODO: 1. 将数据转为 k v (mid,logdate),StartUplog)
    val midAndToLogDStream: DStream[((String, String), StartUpLog)] = fiilterByRedisDStream.map(log => {
      ((log.mid, log.LogDate), log)
    })
    // TODO: 2. 使用groupbykey将相同key的数据聚合到一起
    val midAndDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndToLogDStream.groupByKey()
    // TODO: 3. 对相同的key按照时间排序，并取出第一条
    val sortWithTsDStream: DStream[((String, String), List[StartUpLog])] = midAndDateToLogIterDStream.mapValues(
      it => {
        it.toList.sortWith(_.ts < _.ts).take(1)
      }
    )
    // TODO: 4.将value的List集合打散并返回
    val value: DStream[StartUpLog] = sortWithTsDStream.flatMap(_._2)

    value
  }




  /**
   * todo 批次内去重
   *
   * @param filterByRedisDStream
   */
/*  def filterByMid(filterByRedisDStream:DStream[StartUpLog])={


}*/

  /**
   * 利用redis进行跨批次去重
   * @param startUpDSream
   * @param sc
   */

  def fileterByRedis(startUpDSream:DStream[StartUpLog],sc:SparkContext)={
    // TODO: 方案1: 逐条对数据进行去重操作
    val value: DStream[StartUpLog] = startUpDSream.filter(log => {
      //    a.  获取redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //    b.   设计rediskey
      val rediskey: String = "DAU:" + log.LogDate
      //    c.  拿本批次的mid去redis中对比数据，判断是否存在
      val boolean: lang.Boolean = jedisClient.sismember(rediskey, log.mid)
      //      关闭连接
      jedisClient.close()
      !boolean


    })
    value
}

  /**
   * todo 将去重后的结果mid写入redis，方便下个批次的数据去做去重
   * @param startUpLogDStream
   */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(partition=>{
        // TODO: a.获取redis连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        // TODO: 2. 在foreach算子中将数据写入redis
        partition.foreach(log =>{
          // TODO: rediskey设计
          val rediskey:String ="DAU:"+log.LogDate
          // TODO: 根据key将数据保存至redis
          jedisClient.sadd(rediskey,log.mid)
        })
        jedisClient.close()
      })
    })

  }
}
