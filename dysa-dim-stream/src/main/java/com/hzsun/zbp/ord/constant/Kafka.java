package com.hzsun.zbp.ord.constant;

public class Kafka {


    //broker
    //生产
    public static final String KAFKA_BROKER_LIST = "10.45.207.181:19092" ;

    //public static final String KAFKA_BROKER_LIST = "172.16.1.6:19092";


    //topic
    //    DIM
    public static final String DIM_SCH = "realtime.dim.sch_name";
    public static final String DIM_STAFF = "realtime.dim.staff_num";
    public static final String SCH_PRINT_DIM_TOPIC = "realtime.dim.sch_sample_p";
    public static final String SCH_SAMPLE_DIM_TOPIC = "realtime.dim.sch_sample";


    //    ODS
    public static final String ODS_DETAIL = "realtime_ods_binlog_ord.anpin.ord_order_detail";
    public static final String ODS_DISTRIBUTION = "realtime_ods_binlog_ord.anpin.ord_distribution";


    //     因为要转时间 只能写入表
    public static final String DW_DETAIL = "realtime_dw.ord_order_detail";
    public static final String DW_DISTRIBUTION = "realtime_dw.ord_distribution";


    //     ADS  验收
    public static final String ADS_ORD_TOP = "realtime_ads.ord.top";
    public static final String ADS_ORD_LIST_CDC = "realtime_ads.ord.list_cdc";
    public static final String ADS_ORD_LIST_ALL = "realtime_ads.ord.list_all";


}
