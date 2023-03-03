package com.hzsun.zbp.mc.kafka;

//import org.apache.flink.api.common.serialization.SerializationSchema;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

//  SerializationSchema   X
// KafkaSerializationSchema v
public class KafkaJsonSerializer implements KafkaSerializationSchema<JSONArray> {


    private String topic;
    //public KafkaJsonSerializer(String mcTopAppTopic) {
    //    this.topic = mcTopAppTopic;
    //    //super();
    //}


    public KafkaJsonSerializer(String mcTopAppTopic) {
        this.topic = mcTopAppTopic;
    }

    //@Override
    //public void open(SerializationSchema.InitializationContext context) throws Exception {
    //    KafkaSerializationSchema.super.open(context);
    //}

    @Override
    public ProducerRecord<byte[], byte[]> serialize(JSONArray s, @Nullable Long aLong) {
        String jsonStr = s.toString();
        return new ProducerRecord<>(topic, jsonStr.getBytes());
    }
}
