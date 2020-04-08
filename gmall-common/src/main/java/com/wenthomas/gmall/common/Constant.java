package com.wenthomas.gmall.common;

/**
 * @author Verno
 * @create 2020-03-28 16:48
 */
public class Constant {

    // Kafka主题
    public static final String TOPIC_STARTUP = "gmall_topic_startup";
    public static final String TOPIC_EVENT = "gmall_topic_event";

    // dau 的phoenix表  shift+ctrl+u 大小写切换的快捷键
    public final static String DAU_TABLE = "GMALL_DAU";


    public final static String TOPIC_ORDER_INFO = "topic_order_info";
    public final static String TOPIC_ORDER_DETAIL = "topic_order_detail";
    public final static String TABLE_USER_INFO = "user_info";

    // 实时预警的es index
    public final static String INDEX_ALERT = "gmall_coupon_alert";

    //灵活查询的es index
    public static final String INDEX_SALE_DETAIL = "gmall_sale_detail";
}
