package com.hzsun.zbp.mc.kafka;

import com.alibaba.fastjson.JSONArray;
import com.hzsun.zbp.mc.config.KafkaConfig;
import com.hzsun.zbp.mc.utils.TimeUtils;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaIO {


    //  1、定义私有flinksource 已有的kafka_consumer
    private static FlinkKafkaConsumer<ObjectNode> normalSource;
    //  2、定义私有flinksink 创建新的kafka_producer
    private static FlinkKafkaProducer<String> normalSink;

    private static FlinkKafkaConsumer<ObjectNode> abnormalSource;
    private static FlinkKafkaProducer<String> abnormalSink;

    //app
    private static FlinkKafkaProducer<String> mcTopSink;
    private static FlinkKafkaProducer<String> mcListCSink;
    private static FlinkKafkaProducer<JSONArray> mcListSink;


    static {
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect",KafkaConfig.ZOOKEEPER);
        properties.setProperty("bootstrap.servers",KafkaConfig.KAFKA_BROKER_LIST);
        //properties.setProperty("transaction.timeout.ms",1000*60*5+"");
        //properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("group.id", "test-consumer-group");
        //properties.setProperty("group.id", "flink1");


        normalSource = new FlinkKafkaConsumer<>(KafkaConfig.KAFKA_TOPIC_NORMAL,new JsonNodeDeserializationSchema(),properties);
        normalSource.setStartFromTimestamp(TimeUtils.getYestZeroPointL());
        abnormalSource = new FlinkKafkaConsumer<>(KafkaConfig.KAFKA_TOPIC_ABNORMAL,new JsonNodeDeserializationSchema(),properties);
        abnormalSource.setStartFromTimestamp(TimeUtils.getYestZeroPointL());




        normalSink = new FlinkKafkaProducer<>(KafkaConfig.NORMAL_DW_TOPIC,new Serialization(KafkaConfig.NORMAL_DW_TOPIC),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE );
        abnormalSink = new FlinkKafkaProducer<>(KafkaConfig.ABNORMAL_DW_TOPIC,new Serialization(KafkaConfig.ABNORMAL_DW_TOPIC),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE );

        //app
        mcTopSink = new FlinkKafkaProducer<>(KafkaConfig.MC_TOP_APP_TOPIC, new Serialization(KafkaConfig.MC_TOP_APP_TOPIC),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE );
        mcListCSink = new FlinkKafkaProducer<>(KafkaConfig.MC_LIST_C_APP_TOPIC,  new Serialization(KafkaConfig.MC_LIST_C_APP_TOPIC),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE );
        mcListSink = new FlinkKafkaProducer<>(KafkaConfig.MC_LIST_APP_TOPIC,  new KafkaJsonSerializer(KafkaConfig.MC_LIST_APP_TOPIC),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE );

    }

    //  ods
    public static FlinkKafkaConsumer<ObjectNode> getNormalSource(){return normalSource;}
    public static FlinkKafkaConsumer<ObjectNode> getAbnormalSource(){return abnormalSource;}




    //  dw
    public static FlinkKafkaProducer<String> getNormalSink(){
        return normalSink;
    }
    public static FlinkKafkaProducer<String> getAbnormalSink(){
        return abnormalSink;
    }


    // app
    public static FlinkKafkaProducer<String> getMcTopSink(){
        return mcTopSink;
    }
    public static FlinkKafkaProducer<String> getMcListCdcSink(){
        return mcListCSink;
    }
    public static FlinkKafkaProducer<JSONArray> getMcListSink(){
        return mcListSink;
    }



}
