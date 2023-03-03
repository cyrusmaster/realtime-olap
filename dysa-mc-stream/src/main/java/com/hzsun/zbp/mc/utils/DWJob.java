//package com.hzsun.zbp.mc.utils;
//
//import com.hzsun.zbp.mc.filter.Filter;
//import com.hzsun.zbp.mc.config.KafkaConfig;
//import com.hzsun.zbp.mc.kafka.KafkaIO;
//import com.hzsun.zbp.mc.model.dwd.AbnormalDTO;
//import com.hzsun.zbp.mc.model.dwd.NormalDTO;
//import com.hzsun.zbp.mc.utils.TimeUtils;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.CheckpointConfig;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//
//public class DWJob {
//
//    private static  StreamExecutionEnvironment env;
//    private static EnvironmentSettings environmentSettings;
//    private static StreamTableEnvironment tenv;
//
//
//    public static void main(String[] args) throws Exception {
//
//        env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        //env.enableCheckpointing(5000);
//        //env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000));
//        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//
//
//        env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        //env.setParallelism(1);
//
//
//
//        environmentSettings = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .useBlinkPlanner()
//                .build();
//
//        tenv = StreamTableEnvironment.create(env, environmentSettings);
//
////        normal
////        source
//        DataStreamSource<ObjectNode> normalObjectNode = env.addSource(KafkaIO.getNormalSource());
//
//        //normalObjectNode.print();
//
////        transformation
//        SingleOutputStreamOperator<NormalDTO> normalDTO = normalObjectNode.filter(new FilterFunction<ObjectNode>() {
//            @Override
//            public boolean filter(ObjectNode jsonNodes) throws Exception {
//                return Filter.ToFilter(jsonNodes);
//            }
//        }).map(new MapFunction<ObjectNode, NormalDTO>() {
//            @Override
//            public NormalDTO map(ObjectNode jsonNodes) throws Exception {
//                return new NormalDTO(jsonNodes);
//            }
//        });
//
////       sink
//
//        normalDTO.map(new MapFunction<NormalDTO, String>() {
//            @Override
//            public String map(NormalDTO normalDTO) throws Exception {
//                return normalDTO.toString();
//            }
//        })
//        //        .print();
//                .addSink(KafkaIO.getNormalSink());
//
//
////        abnormal
////        source
//        DataStreamSource<ObjectNode> objectNodeDataStreamSource = env.addSource(KafkaIO.getAbnormalSource());
//
//
//
////        transformation
//
//        SingleOutputStreamOperator<AbnormalDTO> map = objectNodeDataStreamSource.filter(new FilterFunction<ObjectNode>() {
//            @Override
//            public boolean filter(ObjectNode jsonNodes) throws Exception {
//                return Filter.ToFilter(jsonNodes);
//            }
//        }).map(new MapFunction<ObjectNode, AbnormalDTO>() {
//
//            @Override
//            public AbnormalDTO map(ObjectNode jsonNodes) throws Exception {
//                return new AbnormalDTO(jsonNodes);
//            }
//        });
//
////        sink
////        map.map(new MapFunction<AbnormalDTO, String>() {
////            @Override
////            public String map(AbnormalDTO abnormalDTO) throws Exception {
////                return abnormalDTO.toString();
////            }
////        })
////                .print();
//
//                //.addSink(KafkaIO.getAbnormalSink());
//
//
//
//
//     //source 1  latest  earliest
//        String normalSource = "CREATE TABLE normal (\n" +
//                "    id STRING,\n" +
//                "    cateringId STRING,\n" +
//                "    safeterId STRING,\n" +
//                "    deviceNo STRING,\n" +
//                "    signTime STRING,\n" +
//                "    signType STRING,\n" +
//                "    temperature STRING \n" +
//                //"    prossTime AS TO_TIMESTAMP(signTime),\n" +
//                //  事件时间语义： 即使乱序 或者迟到 也可保证窗口结果一致性
//                //  允许5s 延迟（严格保证结果一致）
//                //"     WATERMARK FOR prossTime AS prossTime - INTERVAL '1' SECOND \n" +
//                //  处理：计算列（机器时间） 作为时间语意    无需提取时间戳和水印
//                //"     prossTime AS PROCTIME() \n" +
//                ") WITH (\n" +
//                "    'connector' = 'kafka',\n" +
//                //"    'connector.version' = 'universal',\n" +
//                "    'topic' = '"+ KafkaConfig.NORMAL_DW_TOPIC +"',\n" +
//                //"    'scan.startup.mode' = 'latest-offset',\n" +
//                "    'scan.startup.mode' = 'timestamp',\n" +
//                "    'scan.startup.timestamp-millis' = '" + TimeUtils.getTodayZeroPointS()+"',\n" +
//                //"    'connector.properties.zookeeper.connect' = '"+ KafkaConfig.ZOOKEEPER +"',\n" +
//                "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"'  ,\n" +
//                "    'format' = 'csv'\n" +
//                ")";
//        tenv.executeSql(normalSource);
//
//
////     source 2
//        String abnormalSource = "CREATE TABLE abnormal (\n" +
//                "    id STRING,\n" +
//                "    cateringId STRING,\n" +
//                "    safeterId STRING,\n" +
//                "    deviceNo STRING,\n" +
//                "    signTime STRING,\n" +
//                "    type STRING,\n" +
//                "    fhand STRING,\n" +
//                "    bhand STRING,\n" +
//                "    description STRING,\n" +
//                "    fhandInfo STRING,\n" +
//                "    bhandInfo STRING,\n" +
//                "    enquiryItem STRING,\n" +
//                "    audit STRING,\n" +
//                "    auditMessage STRING \n" +
//                //"    prossTime AS TO_TIMESTAMP(signTime),\n" +
//                //"    WATERMARK FOR prossTime AS prossTime - INTERVAL '1' SECOND \n" +
//                // 增加格外的列  处理时间属性
//                // flink 接受到时间 不需要提取时间戳和定义水位线
//                //"   prossTime AS PROCTIME()  " +
//                ") WITH (\n" +
//                "    'connector' = 'kafka',\n" +
//                //"    'connector.version' = 'universal',\n" +
//                "    'topic' = '"+ KafkaConfig.ABNORMAL_DW_TOPIC +"',\n" +
//                //"    'scan.startup.mode' = 'latest-offset',\n" +
//                "    'scan.startup.mode' = 'timestamp',\n" +
//                "    'scan.startup.timestamp-millis' = '" +TimeUtils.getTodayZeroPointS()+"',\n" +
//                //"    'connector.properties.zookeeper.connect' = '"+ KafkaConfig.ZOOKEEPER +"',\n" +
//                "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"'  ,\n" +
//                "    'format' = 'csv'\n" +
//                ")";
////
//        tenv.executeSql(abnormalSource);
//
//
//        tenv.executeSql( "CREATE TABLE   mc_cdc (\n" +
//                "    cateringId STRING,\n" +
//                "    safeterId STRING,\n" +
//                "    signTime STRING,\n" +
//                "    flag STRING ,\n" +
//                // 默认 窗口开始时间为0时0分  否则需要将事件时间往后推移
//                //"    eventTime  AS PROCTIME()\n" +
//                "    eventTime AS TO_TIMESTAMP(signTime),\n" +
//                "    WATERMARK FOR eventTime AS eventTime - INTERVAL '3' SECOND \n" +
//                ") WITH (\n" +
//                "    'connector' = 'kafka',\n" +
//                //"    'topic' = 'mc_cdc',\n" +
//                "    'topic' = '"+ KafkaConfig.DWS_MC_MERGE +"',\n" +
//                //"    'scan.startup.mode' = 'latest-offset',\n" +
//                //"    'scan.startup.mode' = 'timestamp',\n" +
//                //"    'scan.startup.timestamp-millis' = '" +TimeUtils.getTodayZeroPointS()+"',\n" +
//                "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"'  ,\n" +
//                "    'format' = 'csv'\n" +
//                ")");
//
//
//        tenv.executeSql("INSERT INTO  mc_cdc  SELECT cateringId,safeterId,signTime,'2' FROM abnormal");
//
//        tenv.executeSql("INSERT INTO  mc_cdc  SELECT cateringId,safeterId,signTime,'1' FROM normal");
//
//
//
//
//        env.execute("MC-DWJob-1.1");
//
//
//
//
//    }
//
//
//
//
//}
