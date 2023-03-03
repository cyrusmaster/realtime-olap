package com.hzsun.zbp.mc.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.util.Collector;


public class WinTest {
    public static void main(String[] args)throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());





        //env.executeAsync()


        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("172.16.67.251", 7777);


         //mark 1



        //stringDataStreamSource.print().setParallelism(1);


        // mark 2
        //
        //stringDataStreamSource.map(new MapFunction<String, String[]>() {
        //    @Override
        //    public String[] map(String s) throws Exception {
        //
        //        String[] split = s.split(",");
        //        //Double aDouble = Double.valueOf(split[2]);
        //
        //
        //        return split;
        //    }
        //}).map(new MapFunction<String[], Tuple3<String,Long,Double>>() {
        //    @Override
        //    public Tuple3<String,Long,Double> map(String[] strings) throws Exception {
        //        return Tuple3.of(strings[0],Long.valueOf(strings[1]),Double.valueOf(strings[2]));
        //    }
        //})
        //        // 无序水印
        //        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Double>>() {
        //    @Override
        //    public long extractTimestamp(Tuple3<String, Long, Double> stringLongDoubleTuple3) {
        //        return stringLongDoubleTuple3.f1*1000L;
        //    }
        //}).keyBy(0)
        //                .timeWindow(Time.seconds(10))
        //                        .reduce(new ReduceFunction<Tuple3<String, Long, Double>>() {
        //                            @Override
        //                            public Tuple3<String, Long, Double> reduce(Tuple3<String, Long, Double> stringLongDoubleTuple3, Tuple3<String, Long, Double> t1) throws Exception {
        //
        //                                double min = t1.f2.min(stringLongDoubleTuple3.f2);
        //                                return t1;
        //                            }
        //                        });







        env.execute();

    }
}
