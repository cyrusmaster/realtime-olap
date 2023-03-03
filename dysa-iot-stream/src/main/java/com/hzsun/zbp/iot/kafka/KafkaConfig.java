package com.hzsun.zbp.iot.kafka;

public class KafkaConfig {

    //线上
    public static final String ZOOKEEPER = "10.45.207.181:6181";
    public static final String KAFKA_BROKER_LIST = "10.45.207.181:19092" ;

    //public static final String ZOOKEEPER = "172.16.1.6:6181";
    //public static final String KAFKA_BROKER_LIST = "172.16.1.6:19092" ;
    //本地
    //public static final String ZOOKEEPER = "172.16.67.251:2181";
    //public static final String KAFKA_BROKER_LIST = "172.16.67.251:9092" ;


    public static final String TOPIC_WARNING = "mysqldy4.anpin.iot_warning" ;
    public static final String TOPIC_SAMPLE = "mysqldy4.anpin.iot_sample" ;
    public static final String TOPIC_SAMPLE_PRINT = "mysqldy4.anpin.iot_sample_print" ;



    //public static final String TOPIC_WARNING_DW = "realtime.dw.iot_warning";

    public static final String SAMPLE_PRINT_DW_TOPIC = "realtime.dw.iot_sample_print";
    public static final String SAMPLE_DW_TOPIC = "realtime.dw.iot_sample";



    public static final String SAMPLE_PRINT_DWB_TOPIC = "realtime.dwb.iot_sample_print";






    public static final String IOT_TOP_APP_TOPIC = "realtime.app.iot_top";
    public static final String IOT_LIST_APP_TOPIC = "realtime.app.iot_list_cdc";
    public static final String IOT_LIST_ALL_APP_TOPIC = "realtime.app.iot_list_all";




//    DIM

    public static final String SCH_PRINT_DIM_TOPIC = "realtime.dim.sch_sample_p";
    public static final String SCH_SAMPLE_DIM_TOPIC = "realtime.dim.sch_sample";





}
