package com.atguigu.constants;

/**
 *  - 声明常量
 */
public class GmallConstants {
    // TODO: 2021/4/16 启动数据去重 
    public static final String KAFKA_TOPIC_STARTUP = "GMALL_STARTUP";
    // TODO: 2021/4/16 订单主题
    //订单主题
    public static final String KAFKA_TOPIC_ORDER = "GMALL_ORDER";
    //TODO 事件日志主题
    public static final String KAFKA_TOPIC_EVENT = "GMALL_EVENT";
    // TODO: 预警日志Index
    public static final String ES_INDEX_ALERT = "gmall_coupon_alert";
    // TODO: 订单详情主题
    public static final String KAFKA_TOPIC_ORDER_DETAIL = "TOPIC_ORDER_DETAIL";
    // TODO: 用户主题
    public static final String KAFKA_TOPIC_USER = "TOPIC_USER_INFO";
    //灵活分析Index
    public static final String ES_INDEX_DETAIL = "gmall2021_sale_detail";

}
