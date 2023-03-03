package com.hzsun.zbp.iot.kafka;

import com.alibaba.fastjson.JSONArray;
import com.hzsun.zbp.iot.utils.TimeUtils;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaIO {


    //   iot_warning  sample_id   iot_sample_print
    private static FlinkKafkaConsumer<ObjectNode>  warningSource;
    private static FlinkKafkaConsumer<ObjectNode> sampleIdSource;
    private static FlinkKafkaConsumer<ObjectNode> samplePrintSource;


    //  dwd json
    //private static FlinkKafkaProducer<String> warningSink;
    //private static FlinkKafkaProducer<String> sampleIdSink;
    //private static FlinkKafkaProducer<String> samplePrintSink;


    private static FlinkKafkaProducer<String> iotTopSink;
    private static FlinkKafkaProducer<String> iotListSink;
    private static FlinkKafkaProducer<JSONArray> iotListAllSink;


    static {
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", com.hzsun.zbp.iot.kafka.KafkaConfig.ZOOKEEPER);
        properties.setProperty("bootstrap.servers", com.hzsun.zbp.iot.kafka.KafkaConfig.KAFKA_BROKER_LIST);
        properties.setProperty("transaction.timeout.ms",1000*60*60+"");
        properties.setProperty("group.id", "test-consumer-group");
        //properties.setProperty("auto.offset.reset", "latest");



        samplePrintSource = new FlinkKafkaConsumer<>(KafkaConfig.TOPIC_SAMPLE_PRINT,new JsonNodeDeserializationSchema(),properties);
        samplePrintSource.setStartFromTimestamp(TimeUtils.getTodayZeroPointL());
        //Long l = 1651766400000l;
        //samplePrintSource.setStartFromTimestamp(l);
        //samplePrintSource.setStartFromEarliest();



        sampleIdSource = new FlinkKafkaConsumer<>(KafkaConfig.TOPIC_SAMPLE,new JsonNodeDeserializationSchema(),properties);
        //sampleIdSource.setStartFromLatest();
        sampleIdSource.setStartFromTimestamp(TimeUtils.getTodayZeroPointL());
        //sampleIdSource.setStartFromTimestamp(l);
        //sampleIdSource.setStartFromEarliest();

        //// 可以自定义反序列化  读取异常插入指定空数据标记 后面判断 但感觉没必要
        //warningSource = new FlinkKafkaConsumer<>(KafkaConfig.TOPIC_WARNING,new JsonNodeDeserializationSchema(),properties);
        ////warningSource.setStartFromLatest();
        //warningSource.setStartFromTimestamp(TimeUtils.getTodayZeroPointL());




        // csv 格式string 存入 方便之后读取
        //warningSink = new FlinkKafkaProducer<String>(KafkaConfig.TOPIC_WARNING_DW,new Serialization(KafkaConfig.TOPIC_WARNING_DW),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE );
        //samplePrintSink = new FlinkKafkaProducer<String>(KafkaConfig.SAMPLE_PRINT_DW_TOPIC,new Serialization(KafkaConfig.SAMPLE_PRINT_DW_TOPIC),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE );
        //sampleIdSink = new FlinkKafkaProducer<String>(KafkaConfig.SAMPLE_DW_TOPIC,new Serialization(KafkaConfig.SAMPLE_DW_TOPIC),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE );


        //ads
        iotTopSink = new FlinkKafkaProducer<>(KafkaConfig.IOT_TOP_APP_TOPIC,  new Serialization(KafkaConfig.IOT_TOP_APP_TOPIC),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE );
        iotListSink = new FlinkKafkaProducer<>(KafkaConfig.IOT_LIST_APP_TOPIC,  new Serialization(KafkaConfig.IOT_LIST_APP_TOPIC),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE );
        iotListAllSink = new FlinkKafkaProducer<>(KafkaConfig.IOT_LIST_ALL_APP_TOPIC,  new KafkaJsonSerializer(KafkaConfig.IOT_LIST_ALL_APP_TOPIC),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE );

    }


    //public static FlinkKafkaConsumer<ObjectNode> getWarningSource(){return warningSource;}

    public static FlinkKafkaConsumer<ObjectNode> getSamplePrintSource(){return samplePrintSource;}
    public static FlinkKafkaConsumer<ObjectNode> getSampleIdSource(){return sampleIdSource;}





    //ads
    public static FlinkKafkaProducer<String> getTopSink(){
        return iotTopSink;
    }
    public static FlinkKafkaProducer<String> getListSink(){
        return iotListSink;
    }
    public static FlinkKafkaProducer<JSONArray> getListAllSink(){
        return iotListAllSink;
    }





    //public static FlinkKafkaProducer<String> getWarningSink(){
    //    return warningSink;
    //}
    //public static FlinkKafkaProducer<String> getSampleIdSink(){
    //    return sampleIdSink;
    //}
    //public static FlinkKafkaProducer<String> getSamplePrintSink(){
    //    return samplePrintSink;
    //}





}
