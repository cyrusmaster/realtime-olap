package com.hzsun.zbp.ord.kafka;


import com.alibaba.fastjson.JSONArray;
import com.hzsun.zbp.ord.constant.Kafka;
import com.hzsun.zbp.ord.utils.TimeUtils;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaIO {




    private static FlinkKafkaConsumer<ObjectNode>  sourceDetail;
    private static FlinkKafkaConsumer<ObjectNode> sourceDistribution;



    private static FlinkKafkaProducer<String> topSink;
    private static FlinkKafkaProducer<String> listSink;
    private static FlinkKafkaProducer<JSONArray> listAllSink;


    static {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", Kafka.KAFKA_BROKER_LIST);
        properties.setProperty("group.id", "flink-consumer-group");
        //properties.setProperty("auto.offset.reset", "latest");


        sourceDetail = new FlinkKafkaConsumer<>(Kafka.ODS_DETAIL,new JsonNodeDeserializationSchema(),properties);
        // 第一次运行 确保今天
        sourceDetail.setStartFromTimestamp(TimeUtils.getTodayZeroPointL());
        // 之后 测试
        //sourceDetail.setStartFromLatest();
        //Long l = 1651766400000l;
        //samplePrintSource.setStartFromTimestamp(l);


        sourceDistribution = new FlinkKafkaConsumer<>(Kafka.ODS_DISTRIBUTION,new JsonNodeDeserializationSchema(),properties);
        sourceDistribution.setStartFromTimestamp(TimeUtils.getTodayZeroPointL());
        //sourceDistribution.setStartFromLatest();



        //ads
        topSink = new FlinkKafkaProducer<>(Kafka.ADS_ORD_TOP,  new Serialization(Kafka.ADS_ORD_TOP),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE );
        listSink = new FlinkKafkaProducer<>(Kafka.ADS_ORD_LIST_CDC,  new Serialization(Kafka.ADS_ORD_LIST_CDC),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE );
        listAllSink = new FlinkKafkaProducer<>(Kafka.ADS_ORD_LIST_ALL,  new KafkaJsonSerializer(Kafka.ADS_ORD_LIST_ALL),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE );

    }


    //source
    public static FlinkKafkaConsumer<ObjectNode> getDetailSource(){return sourceDetail;}
    public static FlinkKafkaConsumer<ObjectNode> getDistributionSource(){return sourceDistribution;}





    //sink
    public static FlinkKafkaProducer<String> getTopSink(){
        return topSink;
    }
    public static FlinkKafkaProducer<String> getListSink(){
        return listSink;
    }
    public static FlinkKafkaProducer<JSONArray> getListAllSink(){
        return listAllSink;
    }







}
