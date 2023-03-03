package com.hzsun.zbp.mc.config;

public class KafkaConfig {



    //线上
    public static final String ZOOKEEPER = "10.45.207.181:6181";
    public static final String KAFKA_BROKER_LIST = "10.45.207.181:19092" ;

    // 6
    //public static final String ZOOKEEPER = "172.16.1.6:6181";
    //public static final String KAFKA_BROKER_LIST = "172.16.1.6:19092" ;

    //本地  kafka 三天清楚策略
    //public static final String ZOOKEEPER = "172.16.67.251:2181";
    //public static final String KAFKA_BROKER_LIST = "172.16.67.251:9092" ;



    //dim
    public static final String DIM_SCH = "realtime_dim.sch_name" ;
    public static final String DIM_STAFF = "realtime_dim.staff_num" ;
    public static final String SCH_PRINT_DIM_TOPIC = "realtime_dim.sch_sample_p";


    //ods    日志
    public static final String KAFKA_TOPIC_NORMAL = "realtime_ods.anpin.mc_normal" ;
    public static final String KAFKA_TOPIC_ABNORMAL = "realtime_ods.anpin.mc_abnormal" ;


    //   dwd 明细 清洗 filter
    public static final String NORMAL_DW_TOPIC = "realtime_dwd.mc_normal" ;
    public static final String ABNORMAL_DW_TOPIC = "realtime_dwd.mc_abnormal" ;

    //  dws  明细汇总  打宽  join union
    public static final String DWS_MC_MERGE = "realtime_dws.mc_merge" ;

    // ads  聚合
    public static final String MC_TOP_APP_TOPIC = "realtime_ads.mc_top" ;
    public static final String MC_LIST_C_APP_TOPIC = "realtime_ads.mc_list_cdc" ;
    public static final String MC_LIST_APP_TOPIC = "realtime_ads.mc_list_all" ;







}
