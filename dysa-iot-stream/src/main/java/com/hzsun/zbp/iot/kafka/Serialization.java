package com.hzsun.zbp.iot.kafka;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class Serialization implements KafkaSerializationSchema<String> {

    private String topic;

    //  构造传入topic名
    public Serialization(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {

        //try {
            return new ProducerRecord<>(topic, s.getBytes());
        //}catch (Exception e){
        //    System.out.println("eadsdasd:"+e);
        //    System.out.println("sasdasda:"+s);
        //    return new ProducerRecord<>(topic, s.getBytes());
        //}
    }

}
