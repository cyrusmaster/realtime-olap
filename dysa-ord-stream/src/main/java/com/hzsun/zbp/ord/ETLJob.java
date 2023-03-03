package com.hzsun.zbp.ord;

import com.hzsun.zbp.ord.constant.Kafka;
import com.hzsun.zbp.ord.filter.Filter;
import com.hzsun.zbp.ord.kafka.KafkaIO;
import com.hzsun.zbp.ord.model.dw.DetailDTO;
import com.hzsun.zbp.ord.model.dw.DistributionDTO;
import com.hzsun.zbp.ord.utils.TimeUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.time.ZoneId;

public class ETLJob {
    public static void main(String[] args) {


        // todo
        //System.setProperty("user.timezone","GMT+0");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        tenv.getConfig().setIdleStateRetention(Duration.ofHours(30));





        //    mysql
        //    tenv.executeSql ( "CREATE TABLE mysql_distribution (\n" +
        //            "    id  STRING ,\n" +
        //            "    catering_id  STRING ,\n" +
        //            "    status  STRING , \n" +
        //            "    accept_time  timestamp , \n" +
        //            "    distribution_date  Date ,\n" +
        //            "    create_time  timestamp  ,\n" +
        //            "    proctime   AS PROCTIME()  \n" +
        //            //"    eventTime AS TO_TIMESTAMP(create_time) , \n" +
        //            //"    WATERMARK FOR eventTime AS eventTime - INTERVAL '3' SECOND \n" +
        //            ") WITH (\n" +
        //            "    'connector' = 'jdbc',\n" +
        //            "    'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
        //            "    'url' = 'jdbc:mysql://172.16.1.6:3307/db_app?serverTimezone=UTC&useUnicode=true&characterEncoding=UTF-8&useSSL=false',\n" +
        //            "    'table-name' = 'ord_distribution_cur',\n" +
        //            "    'username' = 'root',\n" +
        //            "    'password' = 'hz310012' , \n" +
        //            "    'lookup.cache.max-rows' = '10000' , \n" +
        //            "    'lookup.cache.ttl' = '10min'  \n" +
        //            //"    'lookup.max-retries' = '3'  \n" +
        //            ")"  );




        //mark   配送单表


        DataStreamSource<ObjectNode> objectNodeDataStreamSource1 = env.addSource(KafkaIO.getDistributionSource());

        //objectNodeDataStreamSource1.print();

        SingleOutputStreamOperator<Tuple5<String, String, String, String, String>> map1 = objectNodeDataStreamSource1.filter(new FilterFunction<ObjectNode>() {
            @Override
            public boolean filter(ObjectNode jsonNodes) throws Exception {
                return Filter.distributionFilter(jsonNodes);
            }
        }).map(new MapFunction<ObjectNode, DistributionDTO>() {
            @Override
            public DistributionDTO map(ObjectNode jsonNodes) throws Exception {
                return new DistributionDTO(jsonNodes);
            }
        }).map(new MapFunction<DistributionDTO, Tuple5<String, String, String, String, String>>() {
            @Override
            public Tuple5<String, String, String, String, String> map(DistributionDTO distributionDTO) throws Exception {
                return Tuple5.of(distributionDTO.getId(), distributionDTO.getCateringId(), distributionDTO.getStatus(), distributionDTO.getAccept_time(), distributionDTO.getCreate_time());
            }
        });


        //mark 2.11  删除流

        //SingleOutputStreamOperator<Tuple1<String>> map4 = objectNodeDataStreamSource1.filter(new FilterFunction<ObjectNode>() {
        //    @Override
        //    public boolean filter(ObjectNode jsonNodes) throws Exception {
        //        return Filter.distributionDeleteFilter(jsonNodes);
        //    }
        //}).map(new MapFunction<ObjectNode, String>() {
        //    @Override
        //    public String map(ObjectNode jsonNodes) throws Exception {
        //        JsonNode afterData = jsonNodes.get("before");
        //        String id = afterData.get("id").toString().replace("\"", "");
        //        return id;
        //    }
        //}).map(new MapFunction<String, Tuple1<String>>() {
        //    @Override
        //    public Tuple1<String> map(String s) throws Exception {
        //        return Tuple1.of(s);
        //    }
        //});
        //tenv.createTemporaryView("distributionDelete",map4,Expressions.$("id"));
        //
        //
        //
        //mark 2.2  转表


        tenv.createTemporaryView("distribution",map1, Expressions.$("id"), Expressions.$("catering_id"), Expressions.$("status"), Expressions.$("accept_time"), Expressions.$("create_time"),Expressions.$("row_time").rowtime());


        tenv.executeSql("CREATE TABLE  if not exists dw_distribution (\n" +
                "    id  STRING ,\n" +
                "    catering_id  STRING ,\n" +
                "    status  STRING ,\n" +
                "    accept_time  STRING ,\n" +
                "    create_time  STRING ,\n" +
                "    event_time  STRING , \n" +
                "    eventTime AS TO_TIMESTAMP(event_time) , \n" +
                "    WATERMARK FOR eventTime AS eventTime - INTERVAL '3' SECOND \n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '"+ Kafka.DW_DISTRIBUTION +"',\n" +
                "    'scan.startup.mode' = 'timestamp',\n" +
                "    'scan.startup.timestamp-millis' = '" + TimeUtils.getTodayZeroPointS()+"',\n" +
                "    'properties.bootstrap.servers' = '"+ Kafka.KAFKA_BROKER_LIST +"'  ,\n" +
                "    'format' = 'csv'\n" +
                ")");
        tenv.executeSql("INSERT INTO  dw_distribution SELECT id,catering_id,status,accept_time,create_time,CONVERT_TZ(CAST(row_time as  STRING ),'UTC','GMT+08:00')  FROM distribution ");

        //Table table0 = tenv.sqlQuery("select *   from dw_distribution");
        //tenv.toChangelogStream(table0).print("distribution");



        //    mark  明细

        DataStreamSource<ObjectNode> objectNodeDataStreamSource = env.addSource(KafkaIO.getDetailSource());

        //objectNodeDataStreamSource.print();

        SingleOutputStreamOperator<Tuple6<String, String, String, String, String, String>> map = objectNodeDataStreamSource.filter(new FilterFunction<ObjectNode>() {
            @Override
            public boolean filter(ObjectNode jsonNodes) throws Exception {
                return Filter.detailFilter(jsonNodes);
            }
        }).map(new MapFunction<ObjectNode, DetailDTO>() {
            @Override
            public DetailDTO map(ObjectNode jsonNodes) throws Exception {
                return new DetailDTO(jsonNodes);
            }
        }).map(new MapFunction<DetailDTO, Tuple6<String, String, String, String, String, String>>() {
            @Override
            public Tuple6<String, String, String, String, String, String> map(DetailDTO detailDTO) throws Exception {
                return Tuple6.of(detailDTO.getId(), detailDTO.getDistributionId(), detailDTO.getDistributionSkuName(), detailDTO.getValidResult(), detailDTO.getValidQuantity(), detailDTO.getDistributionQuantity());
            }
        });


        //map.print();

        //mark   转表 (事件时间：行记录时间)
        tenv.createTemporaryView("detail",map,Expressions.$("id") , Expressions.$("distribution_id"), Expressions.$("distribution_sku_name"), Expressions.$("valid_result"),  Expressions.$("valid_quantity"),Expressions.$("distribution_quantity"),Expressions.$("row_time").rowtime());

        //Table table0 = tenv.sqlQuery("select * from detail  ");
        //tenv.toChangelogStream(table0).print("detail");


        //tenv.executeSql("drop table dw_detail ");
        tenv.executeSql("CREATE TABLE  if not exists dw_detail (\n" +
                "    id STRING ,\n" +
                "    distribution_id  STRING ,\n" +
                "    distribution_sku_name  STRING ,\n" +
                "    valid_result  STRING ,\n" +
                "    valid_quantity  STRING ,\n" +
                "    distribution_quantity  STRING ,\n" +
                "    event_time  STRING , \n" +
                "    eventTime AS TO_TIMESTAMP(event_time) , \n" +
                "    WATERMARK FOR eventTime AS eventTime - INTERVAL '3' SECOND \n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '"+ Kafka.DW_DETAIL +"',\n" +
                "    'scan.startup.mode' = 'timestamp',\n" +
                "    'scan.startup.timestamp-millis' = '" + TimeUtils.getTodayZeroPointS()+"',\n" +
                "    'properties.bootstrap.servers' = '"+ Kafka.KAFKA_BROKER_LIST +"'  ,\n" +
                "    'format' = 'csv'\n" +
                ")");
        tenv.executeSql("INSERT INTO  dw_detail SELECT id,distribution_id,distribution_sku_name,valid_result,valid_quantity,distribution_quantity,CONVERT_TZ(CAST(row_time as  STRING ),'UTC','GMT+08:00')  FROM detail ");

        //Table table = tenv.sqlQuery(" SELECT * FROM dw_detail ");
        //tenv.toChangelogStream(table).print("detail");


    }

}
