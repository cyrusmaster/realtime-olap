package com.hzsun.zbp.iot;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hzsun.zbp.iot.filter.Filter;
import com.hzsun.zbp.iot.kafka.KafkaIO;
import com.hzsun.zbp.iot.model.ads.ListVO;
import com.hzsun.zbp.iot.model.ads.ListAllVO;
import com.hzsun.zbp.iot.model.ads.TopVO;
import com.hzsun.zbp.iot.model.dwd.SampleCdcDTO;
import com.hzsun.zbp.iot.model.dwd.SamplePrintCdcDTO;
import com.hzsun.zbp.iot.kafka.KafkaConfig;
import com.hzsun.zbp.iot.model.table.ListTO;
import com.hzsun.zbp.iot.model.table.TopTO;
import com.hzsun.zbp.iot.trigger.OneByOneTrigger;
import com.hzsun.zbp.iot.utils.TimeUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Expressions;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class APPJob {

    private static StreamExecutionEnvironment env;
    public static void main(String[] args) throws Exception {

        System.setProperty("log4j.skipJansi","false");
        //System.setProperty("user.timezone","GMT+0");


        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        TableConfig config = tableEnv.getConfig();
        config.setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        config.setIdleStateRetention(Duration.ofHours(30));
        config.getConfiguration().setBoolean("table.exec.emit.early-fire.enabled", true);
        config.getConfiguration().setString("table.exec.emit.early-fire.delay", "3s");






        //——————————————————————————————————————————————————dim————————————————————————————————————————————————————————————


        //      todo 0615 去掉print表统计
        //tableEnv.executeSql("CREATE TABLE   dim_read_sch_print  (" +
        //        "  restaurant_id  string  ," +
        //        "  organ_no  string  ," +
        //        "  organ_name string , " +
        //        "  PRIMARY KEY (restaurant_id) NOT ENFORCED " +
        //        ") " +
        //        "WITH (" +
        //        "    'connector' = 'upsert-kafka',\n" +
        //        "    'topic' = '"+ KafkaConfig.SCH_PRINT_DIM_TOPIC +"',\n" +
        //        "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"'  ,\n" +
        //        "    'key.format' = 'csv',  \n" +
        //        "    'value.format' = 'csv')"
        //);




        //        dim  2
        tableEnv.executeSql("CREATE TABLE   dim_read_sch_sample  (" +
                "  id  string  ," +
                "  organ_no  string  ," +
                "  organ_name string , " +
                "  PRIMARY KEY (id) NOT ENFORCED " +
                ") " +
                "WITH (" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = '"+ KafkaConfig.SCH_SAMPLE_DIM_TOPIC +"',\n" +
                "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"'  ,\n" +
                "    'key.format' = 'csv',  \n" +
                "    'value.format' = 'csv')"
        );



        //—————————————————————————————————————————————cdc—————————————————————————————————————————————————————————————————

        // source  1  SamplePrint
        //DataStreamSource<ObjectNode> objectNodeDataStreamSource1 = env.addSource(KafkaIO.getSamplePrintSource());
        //
        //
        //
        //SingleOutputStreamOperator<ObjectNode> filter1 = objectNodeDataStreamSource1.filter(new FilterFunction<ObjectNode>() {
        //    @Override
        //    public boolean filter(ObjectNode jsonNodes) throws Exception {
        //        return Filter.samplePrintFilter(jsonNodes);
        //    }
        //});
        //
        //
        //SingleOutputStreamOperator<SamplePrintCdcDTO> map = filter1.map(new MapFunction<ObjectNode, SamplePrintCdcDTO>() {
        //    @Override
        //    public SamplePrintCdcDTO map(ObjectNode jsonNodes) throws Exception {
        //
        //        return new SamplePrintCdcDTO(jsonNodes);
        //    }
        //});
        //// TODO 尝试 流转表   代替kafka
        //SingleOutputStreamOperator<Tuple5<String, String, String, String, Long>> map1 = map.map(new MapFunction<SamplePrintCdcDTO, Tuple5<String, String, String, String, Long>>() {
        //    @Override
        //    public Tuple5 map(SamplePrintCdcDTO value) throws Exception {
        //        //new Tuple4<String,String,String,Long>;
        //        return Tuple5.of(value.getCateringId(), value.getSampleId(), value.getFoodId(), value.getStatus(),value.getCreateDate());
        //    }
        //});
        //
        //
        ////fromDataStream 已弃用
        ////Table table = tableEnv.fromDataStream(map,Expressions.$("cateringId"));
        ////tableEnv.createTemporaryView("school_table",table);
        //
        //// 事件时间  rowtime  记录产生时间 （也可以选择 时间字段  该表没有事件时间字段 ） TODO 只能用记录生成时间 少8h
        //tableEnv.createTemporaryView("sample_print",map1,Expressions.$("f0").as("catering_id"),
        //        Expressions.$("f1").as("sample_id"),Expressions.$("f2").as("food_id"),
        //        Expressions.$("f3").as("status"),Expressions.$("f4").as("create_date"),Expressions.$("user_action_time").rowtime());
        //
        //
        //
        ////通过创建connector 表来解决 窗口聚合时区分割问题
        //tableEnv.executeSql("CREATE TABLE  if not exists sample_print_tmp (\n" +
        //        "    catering_id  STRING ,\n" +
        //        "    sample_id  STRING ,\n" +
        //        "    food_id  STRING ,\n" +
        //        "    status  STRING ,\n" +
        //        "    user_action_time  STRING , \n" +
        //        "    eventTime AS TO_TIMESTAMP(user_action_time) , \n" +
        //        "    WATERMARK FOR eventTime AS eventTime - INTERVAL '3' SECOND \n" +
        //        ") WITH (\n" +
        //        "    'connector' = 'kafka',\n" +
        //        "    'topic' = '"+ KafkaConfig.SAMPLE_PRINT_DW_TOPIC +"',\n" +
        //        //"    'scan.startup.mode' = 'latest-offset',\n" +
        //        "    'scan.startup.mode' = 'timestamp',\n" +
        //        "    'scan.startup.timestamp-millis' = '" +TimeUtils.getTodayZeroPointS()+"',\n" +
        //        //"    'scan.startup.timestamp-millis' = '" +"1651766400000"+"',\n" +
        //        "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"'  ,\n" +
        //        "    'format' = 'csv'\n" +
        //        ")");
        //
        //tableEnv.executeSql("INSERT INTO  sample_print_tmp  SELECT catering_id,sample_id,food_id,status,CONVERT_TZ(CAST(user_action_time as  STRING ),'UTC','GMT+08:00')  FROM sample_print ");
        //




        //  ？？？  上表关联 过滤出新表
        //tableEnv.executeSql("CREATE TABLE  if not exists dwb_sample_print (\n" +
        //        "    catering_id  STRING ,\n" +
        //        "    sample_id  STRING ,\n" +
        //        "    food_id  STRING ,\n" +
        //        "    status  STRING ,\n" +
        //        "    user_action_time  STRING , \n" +
        //        "    eventTime AS TO_TIMESTAMP(user_action_time) , \n" +
        //        "    WATERMARK FOR eventTime AS eventTime - INTERVAL '3' SECOND \n" +
        //        ") WITH (\n" +
        //        "    'connector' = 'kafka',\n" +
        //        "    'topic' = '"+ KafkaConfig.SAMPLE_PRINT_DWB_TOPIC +"',\n" +
        //        //"    'scan.startup.mode' = 'latest-offset',\n" +
        //        "    'scan.startup.mode' = 'timestamp',\n" +
        //        "    'scan.startup.timestamp-millis' = '" +TimeUtils.getTodayZeroPointS()+"',\n" +
        //        //"    'scan.startup.timestamp-millis' = '" +"1651766400000"+"',\n" +
        //        "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"'  ,\n" +
        //        "    'format' = 'csv'\n" +
        //        ")");
        //
        //
        //
        //tableEnv.executeSql("INSERT INTO  dwb_sample_print  SELECT catering_id,sample_id,food_id,status,user_action_time  FROM sample_print_tmp  where catering_id in (select restaurant_id from dim_read_sch_print )    ");








        //        source 2   iot_sample
        DataStreamSource<ObjectNode> objectNodeDataStreamSource = env.addSource(KafkaIO.getSampleIdSource());


        //objectNodeDataStreamSource.print();



        //filter.print();

        //SingleOutputStreamOperator<ObjectNode> filter_c = objectNodeDataStreamSource.filter(new FilterFunction<ObjectNode>() {
        //    @Override
        //    public boolean filter(ObjectNode jsonNodes) throws Exception {
        //        return Filter.sampleFilter_c(jsonNodes);
        //    }
        //});
        //
        ////filter_c.print();
        //
        //
        //
        //// mark 插入表（取样菜品数量
        //SingleOutputStreamOperator<Tuple7<String, String, Integer, String, String, String, String>> op1 = filter_c.filter(new FilterFunction<ObjectNode>() {
        //            @Override
        //            public boolean filter(ObjectNode value) throws Exception {
        //                JsonNode data = value.get("op");
        //                String opdate = data.toString().replace("\"", "");
        //                if (!("u".contains(opdate))) {
        //                    //System.out.println("3123");
        //                    return false;
        //                } else {
        //                    return true;
        //                }
        //            }
        //        })
        //        .map(new MapFunction<ObjectNode, SampleCdcDTO>() {
        //            @Override
        //            public SampleCdcDTO map(ObjectNode jsonNodes) throws Exception {
        //                return new SampleCdcDTO(jsonNodes);
        //            }
        //        }).map(new MapFunction<SampleCdcDTO, Tuple7<String, String, Integer, String, String, String, String>>() {
        //            @Override
        //            public Tuple7<String, String, Integer, String, String, String, String> map(SampleCdcDTO value) throws Exception {
        //                return Tuple7.of(value.getId(), value.getPersonId(), value.getServings(), value.getStartDate(), value.getEndDate(), value.getCreateDate(), value.getModifyDate());
        //            }
        //        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple7<String, String, Integer, String, String, String, String>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
        //                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple7<String, String, Integer, String, String, String, String>>() {
        //                    @Override
        //                    public long extractTimestamp(Tuple7<String, String, Integer, String, String, String, String> val, long l) {
        //
        //
        //                        // fixme  这里应该要把时间->毫秒时间戳
        //
        //                        long time = 0;
        //                        try {
        //                            time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA).parse(val.f6).getTime();
        //                        } catch (ParseException e) {
        //                            throw new RuntimeException(e);
        //                        }
        //
        //                        return time;
        //                    }
        //                }));
        //
        //
        //tableEnv.createTemporaryView("sample_c",op1,Expressions.$("f0").as("id"),Expressions.$("f1").as("person_id"),
        //        Expressions.$("f2").as("servings"),
        //        Expressions.$("f3").as("start_date"),Expressions.$("f4").as("end_date"),
        //        Expressions.$("f5").as("create_date"),Expressions.$("modify_date").rowtime()
        //);


        //Table table1 = tableEnv.sqlQuery("select * from sample_c  ");
        //tableEnv.toChangelogStream(table1).print("111");







        SingleOutputStreamOperator<ObjectNode> filter = objectNodeDataStreamSource.filter(new FilterFunction<ObjectNode>() {
            @Override
            public boolean filter(ObjectNode jsonNodes) throws Exception {
                return Filter.sampleFilter(jsonNodes);
            }
        });

        // mark  更新表（取样次数、取样达标次数）
        SingleOutputStreamOperator<Tuple7<String, String, Integer, String, String, String, String>> op = filter.filter(new FilterFunction<ObjectNode>() {
            @Override
            public boolean filter(ObjectNode value) throws Exception {
                JsonNode data = value.get("op");
                String opdate = data.toString().replace("\"", "");
                if ("u".contains(opdate)) {
                    return true;
                } else {
                    return false;
                }
            }
        }).map(new MapFunction<ObjectNode, SampleCdcDTO>() {
            @Override
            public SampleCdcDTO map(ObjectNode jsonNodes) throws Exception {
                return new SampleCdcDTO(jsonNodes);
            }
        }).map(new MapFunction<SampleCdcDTO, Tuple7<String, String, Integer, String, String, String, String>>() {
            @Override
            public Tuple7<String, String, Integer, String, String, String, String> map(SampleCdcDTO value) throws Exception {
                return Tuple7.of(value.getId(), value.getPersonId(), value.getServings(), value.getStartDate(), value.getEndDate(), value.getCreateDate(), value.getModifyDate());
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple7<String, String, Integer, String, String, String, String>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple7<String, String, Integer, String, String, String, String>>() {
                    @Override
                    public long extractTimestamp(Tuple7<String, String, Integer, String, String, String, String> val, long l) {

                        // fixme  这里应该要把时间->毫秒时间戳
                        long time = 0;
                        try {
                            time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA).parse(val.f6).getTime();
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }

                        return time;
                    }
                }));


        //   mark 流转表   事件时间采用modify_date
        tableEnv.createTemporaryView("sample_u",op,Expressions.$("f0").as("id"),Expressions.$("f1").as("person_id"),
                Expressions.$("f2").as("servings"),
                Expressions.$("f3").as("start_date"),Expressions.$("f4").as("end_date"),
                Expressions.$("f5").as("create_date"),Expressions.$("modify_date").rowtime()
                );

        //Table table1 = tableEnv.sqlQuery("select * from sample_u  ");
        //tableEnv.toChangelogStream(table1).print("111");







        //——————————————————————————————————————————————————flink sql———————————————————————————————————————————————————————————————





        //  todo final
        Table top = tableEnv.sqlQuery("select\n" +
                        "    CAST(a.sampleNum as int)  AS sampleNum ,\n" +
                        "    CAST(b.getCnt as int)  AS getCnt ,\n" +
                        //"    c.c AS c ,\n" +
                        "    ROUND((CAST(c.c AS DOUBLE)/b.getCnt *100 ) ,2) AS standardRate ,\n" +
                        "    CAST( CURRENT_TIMESTAMP as timestamp(3) )  as  rowTime\n" +
                        "       from\n" +
                        // (select count(distinct a1.food_id)  AS sampleNum  from  (select catering_id AS catering_id,food_id   AS food_id  from sample_print_tmp   GROUP BY  catering_id ,food_id , TUMBLE(eventTime, INTERVAL '1' DAY) ) a1  where a1.catering_id IN (select restaurant_id from dim_read_sch_print)  ) a    ,\n" +
                        "   (select sum( a1.servings)     AS sampleNum from (select distinct id, servings ,person_id    from sample_u   GROUP BY  id, servings ,person_id , TUMBLE(modify_date, INTERVAL '1' DAY) ) a1  where a1.person_id IN (select id from dim_read_sch_sample)  ) a    ,\n" +
                        "   (select count(distinct b1.id) AS getCnt    from ( select  id AS id , person_id AS person_id  from sample_u   GROUP BY   id , person_id ,TUMBLE(modify_date, INTERVAL '1' DAY)  ) b1 where b1.person_id IN (select id from dim_read_sch_sample)  ) b ,\n" +
                        "   (select count(distinct c1.id) AS c         from (select id AS id ,person_id AS person_id  from  sample_u where  TIMESTAMPDIFF(DAY,TO_TIMESTAMP(start_date),TO_TIMESTAMP(end_date)) >= '2' GROUP BY  id , person_id ,TUMBLE(modify_date, INTERVAL '1' DAY) ) c1 where c1.person_id IN (select id from dim_read_sch_sample)  )c \n"
        );


        //tableEnv.toRetractStream(top, Row.class).print();



        // mark  埋点 测试线上窗口
        //Table top1 = tableEnv.sqlQuery("" +
        //        " select count(distinct b1.id)  AS getCnt ,start_time,end_time  from  ( select  id AS id , person_id AS person_id  ,TUMBLE_START(modify_date,INTERVAL '1' DAY ) as start_time , TUMBLE_END(modify_date,INTERVAL '1' DAY ) as end_time  from sample_u   GROUP BY   id , person_id ,TUMBLE(modify_date, INTERVAL '1' DAY)  ) b1 where b1.person_id IN (select id from dim_read_sch_sample) group by start_time,end_time   ");
        //
        //tableEnv.toChangelogStream(top1).print("111");





        // mark 转流

        DataStream<Tuple2<Boolean, TopTO>> tuple2DataStream = tableEnv.toRetractStream(top, TopTO.class);
        //tuple2DataStream.print();

        SingleOutputStreamOperator<TopTO> map2 = tuple2DataStream.filter(new FilterFunction<Tuple2<Boolean, TopTO>>() {
            @Override
            public boolean filter(Tuple2<Boolean, TopTO> value) throws Exception {
                //if (value.f0 == true && booleanMorningCheckTopTDTOTuple2.f1.okRate <= 100.0 && booleanMorningCheckTopTDTOTuple2.f1.mcRate <= 100.0 ) {
                if ( (value.f0 == false) || (value.f1.standardRate > 100.0) ) {
                    return false;
                } else {
                    return true;
                }
            }
        }).map(new MapFunction<Tuple2<Boolean, TopTO>, TopTO>() {
            @Override
            public TopTO map(Tuple2<Boolean, TopTO> booleanTOPTOTuple2) throws Exception {
                return booleanTOPTOTuple2.f1;
            }
        });
        SingleOutputStreamOperator<String> map3 = map2.map(new MapFunction<TopTO, TopVO>() {
            @Override
            public TopVO map(TopTO topto) throws Exception {
                TopVO topvo = new TopVO(topto.sampleNum, topto.getCnt, topto.standardRate);
                return topvo;
            }
        }).map(new MapFunction<TopVO, String>() {
            @Override
            public String map(TopVO topvo) throws Exception {
                return topvo.toString();
            }
        });



        //————————————————————————————————————————————————————List——————————————————————————————————————————————————————————————



        //  todo final
        Table list = tableEnv.sqlQuery("select\n" +
                        "    a.organ_no  AS schId ,\n" +
                        "    a.organ_name  AS schName ,\n" +
                        "    CAST( COALESCE(b.sampleNum  ,0) as int)  AS sampleNum ,\n" +
                        "    CAST( a.getCnt as int)  AS getCnt ,\n" +
                        //"    c.c AS c ,\n" +
                        "    ROUND((CAST(c.c AS DOUBLE)/a.getCnt *100 ) ,2) AS standardRate ,\n" +
                        "    CAST( CURRENT_TIMESTAMP as timestamp(3) )  as  rowTime\n" +
                        "       from\n" +
                        //  取样次数
                        "   (select organ_no ,organ_name,  count(distinct b1.id)  AS getCnt  from  ( select  id AS id , person_id AS person_id  from sample_u   GROUP BY   id , person_id ,TUMBLE(modify_date, INTERVAL '1' DAY)  ) b1  LEFT JOIN  dim_read_sch_sample  ON dim_read_sch_sample.id = b1.person_id   where b1.person_id IN (select id from dim_read_sch_sample) group by organ_no , organ_name  ) a  \n " +
                        "   LEFT JOIN\n" +
                        //  菜品数
                        "   (select organ_no , organ_name,  sum( a1.servings)  AS sampleNum  from  (select distinct id AS id,servings   AS servings,person_id AS person_id   from sample_u   GROUP BY  id ,servings ,person_id,  TUMBLE(modify_date, INTERVAL '1' DAY) ) a1  LEFT JOIN  dim_read_sch_sample  ON dim_read_sch_sample.id = a1.person_id   where a1.person_id IN (select id from dim_read_sch_sample) group by organ_no , organ_name ) b \n" +
                        "   on b.organ_no = a.organ_no\n" +
                        "   LEFT JOIN\n" +
                        //  取样达标数
                        "   (select organ_no ,organ_name,  count(distinct b1.id)  AS c  from  ( select  id AS id , person_id AS person_id  from sample_u  where TIMESTAMPDIFF(DAY,TO_TIMESTAMP(start_date),TO_TIMESTAMP(end_date)) >= '2'  GROUP BY   id , person_id ,TUMBLE(modify_date, INTERVAL '1' DAY)  ) b1  LEFT JOIN  dim_read_sch_sample  ON dim_read_sch_sample.id = b1.person_id   where b1.person_id IN (select id from dim_read_sch_sample) group by organ_no , organ_name )c \n" +
                        "   on c.organ_no = a.organ_no\n"
        );



        //  win test

        //Table test = tableEnv.sqlQuery("select organ_no , organ_name,  sum( a1.servings)  AS sampleNum ,start_time,end_time  from  (select distinct id AS id,servings   AS servings,person_id AS person_id  ,TUMBLE_START(modify_date,INTERVAL '1' DAY ) as start_time , TUMBLE_END(modify_date,INTERVAL '1' DAY ) as end_time  from sample_u   GROUP BY  id ,servings ,person_id,  TUMBLE(modify_date, INTERVAL '1' DAY) ) a1  LEFT JOIN  dim_read_sch_sample  ON dim_read_sch_sample.id = a1.person_id   where a1.person_id IN (select id from dim_read_sch_sample) group by organ_no , organ_name,start_time,end_time  "
        //);
        //
        //
        //tableEnv.toChangelogStream(test).print("111");


        // mark 转流

        DataStream<Tuple2<Boolean, ListTO>> tuple2DataStream1 = tableEnv.toRetractStream(list, ListTO.class);

        //tuple2DataStream1.print();

        SingleOutputStreamOperator<ListVO> map4 = tuple2DataStream1.filter(new FilterFunction<Tuple2<Boolean, ListTO>>() {
            @Override
            public boolean filter(Tuple2<Boolean, ListTO> booleanListTOTuple2) throws Exception {
                //if (booleanListTOTuple2.f0 == true && booleanMorningCheckTableDTOTuple2.f1.okRate <= 100.0 && booleanMorningCheckTableDTOTuple2.f1.okRate <= 100.0) {

                if ( (!booleanListTOTuple2.f0) || (booleanListTOTuple2.f1.standardRate == null)){
                    return false;
                }else if (booleanListTOTuple2.f1.standardRate > 100.0){
                    return false;

                }else {
                    return true;
                }

            }
        }).map(new MapFunction<Tuple2<Boolean, ListTO>, ListVO>() {
            @Override
            public ListVO map(Tuple2<Boolean, ListTO> value) throws Exception {
                return new ListVO(value.f1.schId, value.f1.schName, value.f1.sampleNum, value.f1.getCnt, value.f1.standardRate,value.f1.rowTime);
            }
        });

        SingleOutputStreamOperator<String> map5 = map4.map(new MapFunction<ListVO, String>() {
            @Override
            public String map(ListVO listVO) throws Exception {
                return listVO.toString();
            }
        });



        //————————————————————————————————————————————————————List All——————————————————————————————————————————————————————————————


        // 全量聚合
        //定义 无水印时间

        SingleOutputStreamOperator<ListVO> map6 = map4.assignTimestampsAndWatermarks(WatermarkStrategy.<ListVO>noWatermarks()
                .withTimestampAssigner(new SerializableTimestampAssigner<ListVO>() {
                    @Override
                    public long extractTimestamp(ListVO listVO, long l) {
                        return listVO.getRowTime().getTime();
                    }
                }));

        //map6.print();


        // 无key 需要windowAll
        // 事件时间窗口必须要定义assignTimestampsAndWatermarks  否则报错
        // 因为事件时间 实际上也是行处理时间  所以预计process 也是一样
        // TODO  窗口时间是啥
        SingleOutputStreamOperator<JSONArray> apply = map6.windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
        //SingleOutputStreamOperator<JSONArray> apply = map6.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
                .trigger(new OneByOneTrigger())
                .apply(new AllWindowFunction<ListVO, JSONArray, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<ListVO> iterable, Collector<JSONArray> collector) throws Exception {

                        //新的对象 因为同对象不能转
                        HashMap<String, ListAllVO> hashMapHashMap = new HashMap<>();
                        for (ListVO listVO : iterable) {

                            if (!(listVO.getSchId() == null)) {

                                ListAllVO listAllVO = new ListAllVO(listVO.getSchId(), listVO.getSchName(), listVO.getSampleNum(), listVO.getGetCnt(), listVO.getStandardRate());

                                hashMapHashMap.put(listVO.getSchId(), listAllVO);

                            }
                        }

                        // 转 JSONArray
                        JSONArray jsonArray = new JSONArray();
                        for (Map.Entry<String, ListAllVO> entry : hashMapHashMap.entrySet()) {
                            JSONObject jsonObject = new JSONObject();
                            if (entry.getValue().getSchId() != null) {

                                jsonObject.put("schId", entry.getValue().getSchId());
                                jsonObject.put("schName", entry.getValue().getSchName());
                                jsonObject.put("sampleNum", entry.getValue().getSampleNum());
                                jsonObject.put("getCnt", entry.getValue().getGetCnt());
                                jsonObject.put("standardRate", entry.getValue().getStandardRate());

                                jsonArray.add(jsonObject);

                                collector.collect(jsonArray);
                            }

                        }

                    }
                });


        //map3.print();
        //map5.print();
        //apply.print();


        map3.addSink(KafkaIO.getTopSink());
        map5.addSink(KafkaIO.getListSink());
        apply.addSink(KafkaIO.getListAllSink());

        env.execute("取样-0705");

    }


}