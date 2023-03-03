package com.hzsun.zbp.mc;

import com.hzsun.zbp.mc.filter.Filter;
import com.hzsun.zbp.mc.config.KafkaConfig;
import com.hzsun.zbp.mc.kafka.KafkaIO;
import com.hzsun.zbp.mc.model.dwd.AbnormalDTO;
import com.hzsun.zbp.mc.model.dwd.NormalDTO;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.ZoneId;

public class ETLJob {



    public static void main(String[] args) throws Exception{
        System.setProperty("log4j.skipJansi","false");


        final  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        TableConfig config = tableEnv.getConfig();
        // 提前输出 sql不支持触发器 只会在窗口后输出
        config.setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        config.setIdleStateRetention(Duration.ofHours(30));



        //  mark    执行优先级1    读取ODS 清洗数据

        DataStreamSource<ObjectNode> normalObjectNode = env.addSource(KafkaIO.getNormalSource());

        SingleOutputStreamOperator<NormalDTO> normalDTO = normalObjectNode.filter(new FilterFunction<ObjectNode>() {
            @Override
            public boolean filter(ObjectNode jsonNodes) throws Exception {
                return Filter.normalToFilter(jsonNodes);
            }
        }).map(new MapFunction<ObjectNode, NormalDTO>() {
            @Override
            public NormalDTO map(ObjectNode jsonNodes) throws Exception {
                return new NormalDTO(jsonNodes);
            }
        });


        //  todo 0609 新增详情需要字段
        normalDTO.map(new MapFunction<NormalDTO, String>() {
                    @Override
                    public String map(NormalDTO normalDTO) throws Exception {
                        return normalDTO.toString();
                    }
                })
                //     map row -> 1 -> sum
                //.map(new MapFunction<String, Integer>() {
                //    @Override
                //    public Integer map(String jsonNodes) throws Exception {
                //        return 1;
                //    }
                //})
                //.keyBy(new KeySelector<Integer,Integer >() {
                //    @Override
                //    public Integer getKey(Integer integer) throws Exception {
                //        return integer;
                //    }
                //})
                //.sum(0).print();

                //.print().setParallelism(1);

                .addSink(KafkaIO.getNormalSink()).setParallelism(1);



        //abnormal
        DataStreamSource<ObjectNode> objectNodeDataStreamSource = env.addSource(KafkaIO.getAbnormalSource());


        //objectNodeDataStreamSource.print();

        SingleOutputStreamOperator<AbnormalDTO> mapSource = objectNodeDataStreamSource.filter(new FilterFunction<ObjectNode>() {
            @Override
            public boolean filter(ObjectNode jsonNodes) throws Exception {
                return Filter.abnormalToFilter(jsonNodes);
            }
        }).map(new MapFunction<ObjectNode, AbnormalDTO>() {

            @Override
            public AbnormalDTO map(ObjectNode jsonNodes) throws Exception {
                return new AbnormalDTO(jsonNodes);
            }
        });

        mapSource.map(new MapFunction<AbnormalDTO, String>() {
                    @Override
                    public String map(AbnormalDTO abnormalDTO) throws Exception {
                        return abnormalDTO.toString();
                    }
                })
                //.writeAsText("D:\\project\\flink\\code\\东阳\\my\\redisVer\\dysa-mc-stream\\src\\main\\resources\\1").setParallelism(1);
                //.print().setParallelism(1);

                .addSink(KafkaIO.getAbnormalSink()).setParallelism(1);

        //.map(new MapFunction<String, Integer>() {
        //    @Override
        //    public Integer map(String jsonNodes) throws Exception {
        //        return 1;
        //    }
        //})
        //        .keyBy(new KeySelector<Integer, Integer>() {
        //            @Override
        //            public Integer getKey(Integer integer) throws Exception {
        //                return integer;
        //            }
        //        })
        //        .sum(0).print();





        //  mark  执行优先级2  读出、合并写入汇总表
        //  fixme 后续修改 ods处理后直接合并写入 （减少两次写 addsink，两次kafka持久化）



        //tableEnv.executeSql("drop table normal ");
        tableEnv.executeSql("CREATE TABLE  if not exists normal (\n" +
                "    id STRING,\n" +
                "    cateringId STRING,\n" +
                "    safeterId STRING,\n" +
                "    deviceNo STRING,\n" +
                "    signTime STRING,\n" +
                "    signType STRING,\n" +
                "    temperature STRING \n" +
                //  事件时间语义： 即使乱序 或者迟到 也可保证窗口结果一致性
                //  允许5s 延迟（严格保证结果一致）
                //"     WATERMARK FOR prossTime AS prossTime - INTERVAL '1' SECOND \n" +
                //  处理：计算列（机器时间） 作为时间语意    无需提取时间戳和水印
                //"     prossTime AS PROCTIME() \n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '"+ KafkaConfig.NORMAL_DW_TOPIC +"',\n" +
                "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"'  ,\n" +
                "    'format' = 'csv'\n" +
                //"    'format' = 'json'\n" +
                ")");
        //tableEnv.executeSql("drop table abnormal ");
        tableEnv.executeSql( "CREATE TABLE  if not exists  abnormal (\n" +
                "    id STRING,\n" +
                "    cateringId STRING,\n" +
                "    safeterId STRING,\n" +
                "    deviceNo STRING,\n" +
                "    signTime STRING,\n" +
                "    type STRING,\n" +
                "    temperature STRING ,\n" +
                "    handPhoto STRING,\n" +
                "    description STRING,\n" +
                "    handInfo STRING,\n" +
                "    enquiryItem STRING,\n" +
                "    audit STRING,\n" +
                "    auditMessage STRING \n" +
                //"    prossTime AS TO_TIMESTAMP(signTime),\n" +
                //"    WATERMARK FOR prossTime AS prossTime - INTERVAL '1' SECOND \n" +
                // 增加格外的列  处理时间属性
                // flink 接受到时间 不需要提取时间戳和定义水位线
                //"   prossTime AS PROCTIME()  " +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '"+ KafkaConfig.ABNORMAL_DW_TOPIC +"',\n" +
                "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"'  ,\n" +
                "    'format' = 'csv'\n" +
                ")");
        //tableEnv.executeSql("drop table mc_merge ");
        tableEnv.executeSql( "CREATE TABLE if not exists  mc_merge (\n" +
                "    cateringId STRING,\n" +
                "    safeterId STRING,\n" +
                "    signTime STRING,\n" +
                "    flag STRING ,\n" +
                "    temperature STRING ,\n" +
                "    faceErroItem STRING ,\n" +
                "    handErroItem STRING ,\n" +
                "    askErroItem STRING ,\n" +
                "    rectificationResults STRING ,\n" +
                "    errPhoto STRING ,\n" +
                // 默认 窗口开始时间为0时0分  否则需要将事件时间往后推移
                //"    eventTime  AS PROCTIME()\n" +
                "    eventTime AS TO_TIMESTAMP(signTime),\n" +
                "    WATERMARK FOR eventTime AS eventTime - INTERVAL '3' SECOND \n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '"+ KafkaConfig.DWS_MC_MERGE +"',\n" +
                "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"'  ,\n" +
                "    'format' = 'csv'\n" +
                ")");


        tableEnv.executeSql("INSERT INTO  mc_merge  SELECT cateringId,safeterId,signTime,'1',temperature,'','','','','' FROM normal");
        tableEnv.executeSql("INSERT INTO  mc_merge  SELECT cateringId,safeterId,signTime,'2',temperature,'',handInfo,enquiryItem,'',handPhoto FROM abnormal");

        //Table table = tableEnv.sqlQuery("SELECT cateringId,safeterId,signTime,'2',temperature,'',handInfo,enquiryItem,'',handPhoto FROM abnormal");

        //tableEnv.toRetractStream(table, Row.class).print();


        env.execute("晨检ETL-0621");



    }



}
