package com.atguigu.beau

/**
  * 用来保存预警数据的
  * @param mid 设备ID
  * @param uids 保存用户ID
  * @param itemIds 保存优惠券所涉及的商品ID
  * @param events 保存用户所涉及的时间
  * @param ts 时间戳
  */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)

