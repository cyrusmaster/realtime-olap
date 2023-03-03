package com.hzsun.zbp.mc;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hzsun.zbp.mc.config.KafkaConfig;
import com.hzsun.zbp.mc.kafka.KafkaIO;
import com.hzsun.zbp.mc.model.*;
import com.hzsun.zbp.mc.trigger.OneByOneTrigger;
import com.hzsun.zbp.mc.utils.TimeUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneId;
import java.util.*;


public class OLAPJob {


    //fixme 123
    //private  static  final Logger lo = LogManager.getLogger(OLAPJob.class);
    public static void main(String[] args) throws Exception {
        // color
        System.setProperty("log4j.skipJansi","flase");
        //String request = "${java:os}";
        //lo.error(request);

        final StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(blinkStreamEnv);
        // standalone 暂且设置
        blinkStreamEnv.setParallelism(1);
        TableConfig config = tableEnv.getConfig();
        config.setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        config.getConfiguration().setBoolean("table.exec.emit.early-fire.enabled", true);
        config.getConfiguration().setString("table.exec.emit.early-fire.delay", "3s");
        //管本次最新结果是否较上次发生变化都下发:
        config.getConfiguration().setBoolean("table.exec.emit.unchanged.enabled",true);
        //针对持续查询 空闲状态保留时间
        config.setIdleStateRetention(Duration.ofHours(30));




        //  mark    执行优先级 1  dim

        tableEnv.executeSql("CREATE TABLE   dim_school_filter   (" +
                "  restaurant_id  string  ," +
                "  organ_no  string  ," +
                "  organ_name string , " +
                "  PRIMARY KEY (restaurant_id) NOT ENFORCED " +
                ") " +
                "WITH (" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = '"+ KafkaConfig.SCH_PRINT_DIM_TOPIC +"',\n" +
                "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"',\n" +
                "    'key.format' = 'csv',  \n" +
                "    'value.format' = 'csv')"
        );


        tableEnv.executeSql("CREATE TABLE    dim_sch  (" +
                "  id  string  ," +
                "  school_id  string  ," +
                "  name string , " +
                "  PRIMARY KEY (id) NOT ENFORCED " +
                ") " +
                "WITH (" +
                //"'connector' = 'print'" +
                "    'connector' = 'upsert-kafka',\n" +
                //"    'topic' = 'realtime.dim.sch_name',\n" +
                "    'topic' = '"+ KafkaConfig.DIM_SCH +"',\n" +
                "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"'  ,\n" +
                //"    'connector.startup-mode' = 'latest-offset',\n" +
                " 'key.format' = 'csv',  \n" +
                " 'value.format' = 'csv')");

        //        dim source 2
        tableEnv.executeSql("CREATE TABLE    dim_staff (" +
                "  catering_id  STRING  ," +
                "  staff_num  BIGINT   , " +
                "  PRIMARY KEY (catering_id) NOT ENFORCED " +
                ") " +
                "WITH (" +
                //"'connector' = 'print'" +
                "    'connector' = 'upsert-kafka',\n" +
                //"    'topic' = 'realtime.dim.staff_num',\n" +
                "    'topic' = '"+ KafkaConfig.DIM_STAFF +"',\n" +
                "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"'  ,\n" +
                //"    'connector.startup-mode' = 'latest-offset',\n" +
                " 'key.format' = 'csv',  \n" +
                " 'value.format' = 'csv')");


        //Table table1 = tableEnv.sqlQuery("SELECT catering_id FROM dim_staff");
        //tableEnv.toChangelogStream(table1).print("test");


        //  mark    执行优先级2 create tmp
        // 用一次就 注掉
        //tableEnv.executeSql("drop table mc_merge ");
        tableEnv.executeSql( "CREATE TABLE if not exists  mc_merge (\n" +
                "    cateringId STRING,\n" +
                "    safeterId STRING,\n" +
                "    signTime bigint,\n" +
                "    flag STRING ,\n" +
                "    temperature STRING ,\n" +
                "    faceErroItem STRING ,\n" +
                "    handErroItem STRING ,\n" +
                "    askErroItem STRING ,\n" +
                "    rectificationResults STRING ,\n" +
                "    errPhoto STRING ,  \n" +
                "    eventTime AS TO_TIMESTAMP(FROM_UNIXTIME(signTime / 1000, 'yyyy-MM-dd HH:mm:ss')) ,\n" +
                "    WATERMARK FOR eventTime AS eventTime - INTERVAL '3' SECOND \n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '"+ KafkaConfig.DWS_MC_MERGE +"',\n" +
                "    'scan.startup.mode' = 'timestamp',\n" +
                //   从kafka 记录产生时间戳读取 和数据事件时间无关 和 kafka数据产生时间有关
                "    'scan.startup.timestamp-millis' = '" +TimeUtils.getTodayZeroPointS()+"',\n" +
                "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"'  ,\n" +
                "    'format' = 'csv'\n" +
                ")");



        //——————————————————————————————————————— top sql ——————————————————————————————————————————————


        //todo  测试 窗口时间

        //Table test  = tableEnv.sqlQuery("SELECT  cateringId,safeterId,signTime,flag ,eventTime, TUMBLE_START(eventTime,INTERVAL '1' DAY ) as start_time , TUMBLE_END(eventTime,INTERVAL '1' DAY ) as start_time  FROM mc_merge  \n" +
        //        "GROUP BY cateringId,safeterId,signTime,flag,eventTime, TUMBLE(eventTime, INTERVAL '1' DAY)   \n" +
        //        "");

        //Table test  = tableEnv.sqlQuery("SELECT  cateringId,safeterId,signTime,flag,TO_TIMESTAMP(FROM_UNIXTIME(signTime / 1000, 'yyyy-MM-dd HH:mm:ss'))   FROM mc_merge  \n" +
        //        "");
        //tableEnv.toRetractStream(test, Row.class).print();




        //  测试窗口  TUMBLE_START(eventTime,INTERVAL '1' DAY ) as start_time , TUMBLE_END(eventTime,INTERVAL '1' DAY ) as end_time

        //Table test = tableEnv.sqlQuery(
        //        "  SELECT  win1.safeterId AS oknum ,start_time,end_time   FROM   (SELECT  safeterId , cateringId ,TUMBLE_START(eventTime,INTERVAL '1' DAY ) as start_time , TUMBLE_END(eventTime,INTERVAL '1' DAY ) as end_time  FROM  mc_merge  WHERE flag ='1' GROUP BY TUMBLE(eventTime, INTERVAL '1' DAY) ,safeterId , cateringId   ) AS win1  where  win1.cateringId IN ( select restaurant_id from  dim_school_filter where organ_no is not null ) \n" +
        //        "");



        //  Final
        //tableEnv.executeSql("drop table mc_merge ");
        Table top = tableEnv.sqlQuery("SELECT\n" +
                "CAST(staff.num as int) AS  num,\n" +
                "CAST(mc.checknum as int)  AS ckNum,\n" +
                "ROUND((CAST(mc.checknum AS DOUBLE)/COALESCE(staff.num , 1) *100 ) ,2) AS mcRate,\n" +
                "ROUND((CAST(ok.oknum AS DOUBLE)/COALESCE(mc.checknum , 1) *100 ) ,2) AS okRate,\n" +
                "CAST( CURRENT_TIMESTAMP as timestamp(3) )  as  rowTime\n" +
                "FROM\n" +
                "(SELECT SUM(staff_num)  AS num FROM dim_staff  ) staff ,\n" +
                "(  SELECT COUNT(DISTINCT win1.safeterId ) AS oknum FROM   (SELECT  safeterId , cateringId ,signTime,flag,temperature  FROM  mc_merge  WHERE flag ='1' GROUP BY TUMBLE(eventTime, INTERVAL '1' DAY) ,safeterId , cateringId ,signTime,flag,temperature   ) AS win1  where  win1.cateringId IN ( select restaurant_id from  dim_school_filter where organ_no is not null ) ) ok ,\n" +
                "(  SELECT COUNT(DISTINCT win2.safeterId ) AS checknum FROM   (SELECT safeterId , cateringId ,signTime,flag,temperature  FROM mc_merge GROUP BY  TUMBLE(eventTime, INTERVAL '1' DAY), safeterId , cateringId ,signTime,flag,temperature    ) AS win2  where  win2.cateringId IN ( select restaurant_id from  dim_school_filter where organ_no is not null )   ) mc \n" +
                "");


        //tableEnv.toRetractStream(test, Row.class).print();


        DataStream<Tuple2<Boolean, MorningCheckTopTDTO>> tuple2DataStream = tableEnv.toRetractStream(top, MorningCheckTopTDTO.class);

        SingleOutputStreamOperator<MorningCheckTopTDTO> map1 = tuple2DataStream.filter(new FilterFunction<Tuple2<Boolean, MorningCheckTopTDTO>>() {
            @Override
            public boolean filter(Tuple2<Boolean, MorningCheckTopTDTO> booleanMorningCheckTopTDTOTuple2) throws Exception {
                if (booleanMorningCheckTopTDTOTuple2.f0 == true) {
                    return true;
                } else {
                    return false;
                }
            }
        }).filter(new FilterFunction<Tuple2<Boolean, MorningCheckTopTDTO>>() {
                    @Override
                    public boolean filter(Tuple2<Boolean, MorningCheckTopTDTO> booleanMorningCheckTopTDTOTuple2) throws Exception {
                        if ( (booleanMorningCheckTopTDTOTuple2.f1.okRate > 100.0) || (booleanMorningCheckTopTDTOTuple2.f1.mcRate > 100.0) ) {
                            return false;
                        } else {
                            return true;
                        }
                    }
                })
                .map(new MapFunction<Tuple2<Boolean, MorningCheckTopTDTO>, MorningCheckTopTDTO>() {
            @Override
            public MorningCheckTopTDTO map(Tuple2<Boolean, MorningCheckTopTDTO> booleanMorningCheckTopTDTOTuple2) throws Exception {
                return booleanMorningCheckTopTDTOTuple2.f1;
            }
        });


        SingleOutputStreamOperator<String> map2 = map1.map(new MapFunction<MorningCheckTopTDTO, MorningCheckTopDTO>() {
            @Override
            public MorningCheckTopDTO map(MorningCheckTopTDTO morningCheckTopTDTO) throws Exception {
                MorningCheckTopDTO morningCheckTopDTO = new MorningCheckTopDTO(morningCheckTopTDTO.num, morningCheckTopTDTO.ckNum, morningCheckTopTDTO.mcRate, morningCheckTopTDTO.okRate, morningCheckTopTDTO.rowTime);
                return morningCheckTopDTO;
            }
        }).map(new MapFunction<MorningCheckTopDTO, String>() {
            @Override
            public String map(MorningCheckTopDTO morningCheckTopDTO) throws Exception {
                return morningCheckTopDTO.toString();
            }
        });



        //——————————————————————————————————————  list sql  ——————————————————————————————————————————————



        //   初版
        //Table table = tableEnv.sqlQuery(" " +
        //        //"select count(id) from (  \n" +
        //        "SELECT  \n" +
        //        "a.name sna , \n" +
        //        "a.school_id id , \n" +
        //        "b.staff_num  totalnum , \n" +
        //        "a.mc_total_num checknum ,  \n" +
        //        "COALESCE( c.mc_fail_num  ,0) errbno , \n" +
        //        //"d.mc_ok_num oknum  \n" +
        //        "if ( a.mc_total_num = 0, 0.00, round((cast (a.mc_total_num  as  double ) / b.staff_num  *100),2)) ckRate ,\n" +
        //        "if ( d.mc_ok_num = 0, 0.00, round((cast (d.mc_ok_num  as  double ) / a.mc_total_num  *100),2)) ckRate \n" +
        //        "FROM \n" +
        //        //"(SELECT cateringId ,COUNT(DISTINCT safeterId )  AS mc_total_num FROM mc_merge group by cateringId) a \n" +
        //        "(SELECT name, school_id ,COUNT(DISTINCT safeterId)  AS mc_total_num FROM mc_merge LEFT JOIN dim_sch ON dim_sch.id = mc_merge.cateringId  group by name,school_id) a \n" +
        //
        //        "LEFT JOIN \n" +
        //        //"(SELECT catering_id, staff_num AS staff_num FROM dim_staff) b \n" +
        //        "(SELECT school_id, SUM(staff_num) AS staff_num FROM dim_staff LEFT JOIN dim_sch ON dim_sch.id = dim_staff.catering_id group by school_id) b \n" +
        //        "ON b.school_id = a.school_id \n" +
        //        "LEFT JOIN \n" +
        //        //"(SELECT cateringId, COUNT(DISTINCT safeterId ) AS mc_fail_num  FROM mc_merge WHERE flag ='2' AND safeterId NOT IN (SELECT safeterId FROM mc_merge WHERE flag ='1') group by cateringId) c \n" +
        //        "(SELECT school_id, COUNT(DISTINCT safeterId ) AS mc_fail_num  FROM mc_merge LEFT JOIN dim_sch ON dim_sch.id = mc_merge.cateringId WHERE flag ='2' AND safeterId NOT IN (SELECT safeterId FROM mc_merge WHERE flag ='1') group by school_id) c \n" +
        //        "ON c.school_id = a.school_id \n" +
        //        "LEFT JOIN \n" +
        //        //"(SELECT cateringId ,COUNT(DISTINCT safeterId )  AS mc_ok_num FROM mc_merge  WHERE flag ='1' group by cateringId )d \n" +
        //        "(SELECT school_id ,COUNT(DISTINCT safeterId )  AS mc_ok_num FROM mc_merge LEFT JOIN dim_sch ON dim_sch.id = mc_merge.cateringId WHERE mc_merge.flag ='1' group by school_id )d \n" +
        //        "ON d.school_id = a.school_id \n" +
        //        //")" +
        //        "");


        // Final
        Table finalTable = tableEnv.sqlQuery(" SELECT  \n" +
                "a.name sna , \n" +
                "a.school_id id , \n" +
                "cast(b.staff_num as int )  totalnum , \n" +
                "cast(a.mc_total_num as int ) checknum ,  \n" +
                // 修改逻辑  由从表直接统计不合格-》全部-合格
                "cast( ( COALESCE( a.mc_total_num  , 0)  -  COALESCE( d.mc_ok_num  , 0)  ) as int)   errbno , \n" +
                //"d.mc_ok_num oknum , \n" +
                "if ( a.mc_total_num = 0, 0.00, round((cast (a.mc_total_num  as  double ) / b.staff_num  *100),2)) ckRate ,\n" +
                "if ( d.mc_ok_num = 0, 0.00, round((cast (d.mc_ok_num  as  double ) / a.mc_total_num  *100),2)) okRate ,\n" +
                "cast( CURRENT_TIMESTAMP as timestamp )  as  row_time \n" +
                "FROM \n" +

                //"(SELECT name, school_id ,COUNT(DISTINCT safeterId)  AS mc_total_num FROM  (select  safeterId ,cateringId ,signTime,flag,temperature  from mc_merge group by  safeterId ,cateringId,signTime,flag,temperature,TUMBLE(eventTime, INTERVAL '1' DAY) ) AS mc   LEFT JOIN    dim_sch ON dim_sch.id = mc.cateringId  group by name,school_id ) a \n" +
                "(SELECT organ_name AS name, organ_no AS  school_id ,COUNT(DISTINCT safeterId)  AS mc_total_num FROM  (select  safeterId ,cateringId ,signTime,flag,temperature  from mc_merge group by  safeterId ,cateringId,signTime,flag,temperature,TUMBLE(eventTime, INTERVAL '1' DAY) ) AS mc   LEFT JOIN    dim_school_filter ON dim_school_filter.restaurant_id = mc.cateringId where dim_school_filter.organ_no is not null   group by organ_name,organ_no ) a \n" +
                "LEFT JOIN \n" +

                //"(SELECT school_id, SUM(staff_num) AS staff_num FROM dim_staff LEFT JOIN dim_sch ON dim_sch.id = dim_staff.catering_id group by school_id) b \n" +
                "(SELECT organ_no AS school_id, SUM(staff_num) AS staff_num FROM dim_staff LEFT JOIN dim_school_filter ON dim_school_filter.restaurant_id = dim_staff.catering_id group by organ_no) b \n" +
                "ON b.school_id = a.school_id \n" +
                "LEFT JOIN \n" +

                //"(SELECT cateringId, COUNT(DISTINCT safeterId ) AS mc_fail_num  FROM mc_merge WHERE flag ='2' AND safeterId NOT IN (SELECT safeterId FROM mc_merge WHERE flag ='1') group by cateringId) c \n" +
                //"(SELECT school_id, COUNT(DISTINCT safeterId ) AS mc_fail_num  FROM (select  safeterId ,cateringId,flag  from mc_merge group by  safeterId ,cateringId,flag,TUMBLE(eventTime, INTERVAL '1' DAY) ) AS mc LEFT JOIN dim_sch ON dim_sch.id = mc.cateringId WHERE flag ='2' AND safeterId NOT IN (SELECT safeterId FROM (select  safeterId ,flag  from mc_merge group by  safeterId ,flag,TUMBLE(eventTime, INTERVAL '1' DAY) ) AS mc WHERE mc.flag ='1') group by school_id) c \n" +
                //"ON c.school_id = a.school_id \n" +
                //"LEFT JOIN \n" +


                //"(SELECT school_id ,COUNT(DISTINCT safeterId )  AS mc_ok_num FROM (select  safeterId ,cateringId,flag,signTime,temperature from mc_merge group by  safeterId ,cateringId,flag, signTime,temperature,  TUMBLE(eventTime, INTERVAL '1' DAY) ) AS mc LEFT JOIN     dim_sch ON dim_sch.id = mc.cateringId WHERE mc.flag ='1' group by school_id )d \n" +
                "(SELECT organ_no AS school_id ,COUNT(DISTINCT safeterId )  AS mc_ok_num FROM (select  safeterId ,cateringId,flag,signTime,temperature from mc_merge group by  safeterId ,cateringId,flag, signTime,temperature,  TUMBLE(eventTime, INTERVAL '1' DAY) ) AS mc LEFT JOIN     dim_school_filter ON dim_school_filter.restaurant_id = mc.cateringId WHERE mc.flag ='1' and dim_school_filter.organ_no is not null   group by organ_no )d \n" +
                "ON d.school_id = a.school_id \n" +
                "");


        //Table test = tableEnv.sqlQuery("SELECT school_id ,  mc.safeterId , mc.start_time, mc.end_time  FROM (select  safeterId ,cateringId,flag ,TUMBLE_START(eventTime,INTERVAL '1' DAY ) as start_time , TUMBLE_END(eventTime,INTERVAL '1' DAY ) as end_time  from mc_merge group by  safeterId ,cateringId,flag,TUMBLE(eventTime, INTERVAL '1' DAY) ) AS mc LEFT JOIN     dim_sch ON dim_sch.id = mc.cateringId WHERE mc.flag ='1'  ");

        //tableEnv.toRetractStream(test, Row.class).print();

        //tableEnv.toDataStream(table).print();






        // mark   listcdc

        DataStream<Tuple2<Boolean, MorningCheckTableDTO>> tuple3DataStream = tableEnv.toRetractStream(finalTable, MorningCheckTableDTO.class);

        SingleOutputStreamOperator<MorningCheckListDTO> map = tuple3DataStream.filter(new FilterFunction<Tuple2<Boolean, MorningCheckTableDTO>>() {
            @Override
            public boolean filter(Tuple2<Boolean, MorningCheckTableDTO> booleanMorningCheckTableDTOTuple2) throws Exception {
                if (booleanMorningCheckTableDTOTuple2.f0 == false ) {
                    return false;
                } else {
                    return true;
                }
            }
        }).filter(new FilterFunction<Tuple2<Boolean, MorningCheckTableDTO>>() {
                    @Override
                    public boolean filter(Tuple2<Boolean, MorningCheckTableDTO> val) throws Exception {
                        if ( (val.f1.id == null) ||  (val.f1.okRate == null) ||  (val.f1.errbno == null) ||  (val.f1.ckRate == null) ){
                            return false;
                        }else {
                            return true;
                        }
                    }
                })
                .filter(new FilterFunction<Tuple2<Boolean, MorningCheckTableDTO>>() {
                    @Override
                    public boolean filter(Tuple2<Boolean, MorningCheckTableDTO> val ) throws Exception {
                        if (  val.f1.errbno < 0 ){
                            return false;
                        }else {
                            return true;
                        }
                    }
                })
                .filter(new FilterFunction<Tuple2<Boolean, MorningCheckTableDTO>>() {
                    @Override
                    public boolean filter(Tuple2<Boolean, MorningCheckTableDTO> val2) throws Exception {
                        if ( val2.f1.ckRate > 100.0  ){
                            return false;
                        }else {
                            return true;
                        }
                    }
                })
                .filter(new FilterFunction<Tuple2<Boolean, MorningCheckTableDTO>>() {
                    @Override
                    public boolean filter(Tuple2<Boolean, MorningCheckTableDTO> val3) throws Exception {
                        if ( val3.f1.okRate  > 100.0 ){
                            return false;
                        }else {
                            return true;
                        }
                    }
                })
                .map(new MapFunction<Tuple2<Boolean, MorningCheckTableDTO>, MorningCheckListDTO>() {
            @Override
            public MorningCheckListDTO map(Tuple2<Boolean, MorningCheckTableDTO> value) throws Exception {
                MorningCheckListDTO ResultDTO = new MorningCheckListDTO(value.f1.sna, value.f1.id, value.f1.totalnum, value.f1.checknum, value.f1.errbno, value.f1.ckRate, value.f1.okRate, value.f1.row_time);
                return ResultDTO;
            }
        });


        //map.print();

        SingleOutputStreamOperator<String> map3 = map.map(new MapFunction<MorningCheckListDTO, String>() {
            @Override
            public String map(MorningCheckListDTO morningCheckListDTO) throws Exception {
                return morningCheckListDTO.toString();
            }
        });







        //      mark   List All
        //       fixme  考虑使用增量聚合 而非全量聚合
        //        只能用窗口聚合  (不应该用 全量聚合（apply process数据都到齐在计算 平均 最大 最小）  应该用增量聚合（reduce，aggregate，sum）   )

        SingleOutputStreamOperator<JSONArray> apply = map.windowAll(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(new OneByOneTrigger())
                .apply(new AllWindowFunction<MorningCheckListDTO, JSONArray, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<MorningCheckListDTO> iterable, Collector<JSONArray> collector) throws Exception {


                        HashMap<String, ShowMCListDTO> hashMapHashMap = new HashMap<>();
                        for (MorningCheckListDTO resultDTO : iterable) {
                            // 拿到 map k  v
                            //String s = resultDTO.toString();
                            ShowMCListDTO showMCListDTO = new ShowMCListDTO();
                            if (!(resultDTO.sna == null)) {
                                String sna = resultDTO.getSna();

                                showMCListDTO.setSna(resultDTO.sna);
                                showMCListDTO.setId(resultDTO.id);
                                showMCListDTO.setTotalnum(resultDTO.totalnum);
                                showMCListDTO.setChecknum(resultDTO.checknum);
                                showMCListDTO.setErrbno(resultDTO.errbno);
                                showMCListDTO.setCkRate(resultDTO.ckRate);
                                showMCListDTO.setOkRate(resultDTO.okRate);

                                hashMapHashMap.put(sna, showMCListDTO);
                            }
                        }


                        JSONArray jsonArray = new JSONArray();

                        //    遍历 HashMap   v 组成 JSONArray
                        for (Map.Entry<String, ShowMCListDTO> entry : hashMapHashMap.entrySet()) {

                            JSONObject jsonObject = new JSONObject();
                            if (entry.getValue().getSna() != null) {

                                jsonObject.put("schId", entry.getValue().getSna());
                                jsonObject.put("id", entry.getValue().getId());
                                jsonObject.put("staffNum", entry.getValue().getTotalnum());
                                jsonObject.put("checkNum", entry.getValue().getChecknum());
                                jsonObject.put("noPassNum", entry.getValue().getErrbno());
                                jsonObject.put("execRate", entry.getValue().getCkRate());
                                jsonObject.put("passRate", entry.getValue().getOkRate());

                                //String str = JSON.toJSONString(jsonObject, SerializerFeature.DisableCircularReferenceDetect);
                                //JSONObject jsonjson = JSON.parseObject(str);

                                jsonArray.add(jsonObject);

                                collector.collect(jsonArray);

                            }

                        }

                    }
                });


        // mark  detail
        // 1  最里层  晨检一条信息



        // 偶尔乱序
        //map2.print();
        //map3.print();
        //apply.print();

        map2.addSink(KafkaIO.getMcTopSink());
        map3.addSink(KafkaIO.getMcListCdcSink());
        apply.addSink(KafkaIO.getMcListSink());



        //map2.writeToSocket("172.16.67.251", 9879, new SerializationSchema<String>() {
        //    @Override
        //    public byte[] serialize(String s) {
        //        return s.getBytes(StandardCharsets.UTF_8);
        //    }
        //});




        blinkStreamEnv.execute("晨检OLAP-0705");




    }



}
