package com.qiuhaijun.constants;

/**
 * @ClassName GmallContants
 * @Description TODO
 * @Author qiuhaijun
 * @Date 2020/2/18 21:19
 * @Version 1.0
 **/

/**
 * 工具类和常量类都可以放在common模块中。
 *
 * 定义一个常量类，用它的属性来代替我们kafka主题。避免出错
 * */
public class GmallConstants {
    //定义启动主题和事件主题
    public static final String KAFKA_TOPIC_STARTUP="GMALL_STARTUP";
    public static final String KAFKA_TOPIC_EVENT="GMALL_EVENT";

    public static final String KAFKA_TOPIC_NEW_ORDER="GMALL_NEW_ORDER";
    public static final String KAFKA_TOPIC_ORDER_DETAIL="GMALL_ORDER_DETAIL";

    public static final String ES_INDEX_DAU="gmall2019_dau";
    public static final String ES_INDEX_NEW_MID="gmall2019_new_mid";
    public static final String ES_INDEX_NEW_ORDER="gmall2019_new_order";
    public static final String ES_INDEX_SALE_DETAIL="gmall2019_sale_detail";

}

