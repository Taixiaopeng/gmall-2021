package com.atguigu.handler

import com.atguigu.beau.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.lang

object DauHandler1 {


  /**
   * 利用redis进行跨批次去重
   * @param startUpLogDStream
   * @param sc
   */
  def filterByRedis(startUpLogDStream:DStream[StartUpLog],sc:SparkContext): Unit ={
    // TODO: 对数据进行去重操作(方案1)
    startUpLogDStream.filter(log=>{
      // TODO: 获取redis连接
      val client: Jedis = RedisUtil.getJedisClient
      // TODO: rediskey
      val rediskey:String = "DAU:"+log.LogDate
      // TODO: 拿本批次的mid去redis中对比数据,判断是否存在
      val boolean: lang.Boolean = client.sismember(rediskey, log.mid)
      // TODO: 关闭连接
      client.close()
      !boolean
    })
  }



  /**
   * 将去重后的结果mid写入Redis,方便下个批次的数据做去重
   * @param startUpLogDStrem
   */
  def saveMidToRedis(startUpLogDStrem:DStream[StartUpLog])={
    startUpLogDStrem.foreachRDD(rdd=>{//写库操作使用foreachRDD
      rdd.foreachPartition(parititon=>{
        // TODO: 1. 获取redis连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        // TODO: 在foreach算子中将数据写入redis
        parititon.foreach(log=>{
          //rediskey
          val rediskey:String = "DAU:"+log.LogDate
          //todo 根据key将数据保存至redis
          jedisClient.sadd(rediskey,log.mid)

        })
        jedisClient.close()
      })

       })


  }
}
