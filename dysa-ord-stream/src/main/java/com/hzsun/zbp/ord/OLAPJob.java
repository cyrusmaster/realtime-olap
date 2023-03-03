package com.hzsun.zbp.ord;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hzsun.zbp.ord.constant.Kafka;
import com.hzsun.zbp.ord.filter.Filter;
import com.hzsun.zbp.ord.kafka.KafkaIO;
import com.hzsun.zbp.ord.model.ads.ListAllVO;
import com.hzsun.zbp.ord.model.ads.ListVO;
import com.hzsun.zbp.ord.model.ads.TopVO;
import com.hzsun.zbp.ord.model.dw.DetailDTO;
import com.hzsun.zbp.ord.model.dw.DistributionDTO;
import com.hzsun.zbp.ord.model.table.ListTableDTO;
import com.hzsun.zbp.ord.model.table.TopTableDTO;
import com.hzsun.zbp.ord.trigger.OneByOneTrigger;
import com.hzsun.zbp.ord.utils.TimeUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/**
  * REMARK    数仓    etl + OLAP
  * @className   ETLJob
  * @date  2022/5/10 22:28
  * @author  cyf
  */
public class OLAPJob {
    public static void main(String[] args) throws Exception {


        // todo
        System.setProperty("log4j.skipJansi","false");
        //System.setProperty("user.timezone","GMT+0");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        tenv.getConfig().setIdleStateRetention(Duration.ofHours(30));
        tenv.getConfig().getConfiguration().setBoolean("table.exec.emit.early-fire.enabled", true);
        tenv.getConfig().getConfiguration().setString("table.exec.emit.early-fire.delay", "3s");


    //    mark 0 dim

        tenv.executeSql("CREATE TABLE   dim_school   (" +
                "  restaurant_id  string  ," +
                "  organ_no  string  ," +
                "  organ_name string , " +
                "  PRIMARY KEY (restaurant_id) NOT ENFORCED " +
                ") " +
                "WITH (" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = '"+ Kafka.SCH_PRINT_DIM_TOPIC +"',\n" +
                "    'properties.bootstrap.servers' = '"+ Kafka.KAFKA_BROKER_LIST +"'  ,\n" +
                "    'key.format' = 'csv',  \n" +
                "    'value.format' = 'csv')"
        );


        //mark   配送单表
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
        //Table table = tenv.sqlQuery("select *   from dw_distribution");
        //tenv.toChangelogStream(table).print("distribution");


        //    mark  明细
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
        //Table table = tenv.sqlQuery(" SELECT * FROM dw_detail ");
        //tenv.toChangelogStream(table).print("detail");


        //  mark 3.1      flink sql top

        //  top  todo 过滤删除
        Table finalTop = tenv.sqlQuery("select   " +
                //"        COALESCE(acceptnum.accept_num,0)    , \n" +
                //"         startnum.start_num  , \n" +
                "         if ( COALESCE(startnum.start_num,0) = 0 , 0.00, ROUND((CAST(COALESCE(acceptnum.accept_num,0) AS DOUBLE)/startnum.start_num *100 ) ,2) )   AS executeRate  , \n" +
                "         if ( COALESCE(check_num.check_num,0) = 0 , 0.00, ROUND((CAST(COALESCE(pass_num.pass_num,0) AS DOUBLE)/check_num.check_num *100 ) ,2) )  AS qualifiedRate  ,  \n" +
                "         ROUND((CAST(CAST(pass_num.pass_num AS INTEGER ) -  CAST(quantity_pass_num.quantity_pass_num AS INTEGER ) AS DOUBLE)/pass_num.pass_num *100 ) ,2)  AS notStandardRate  , \n" +
                "         CAST( CURRENT_TIMESTAMP as timestamp(3) )  as  rowTime\n" +
                "         from  \n" +
                //  验收单数 acceptnum
                //"         ( select count(distinct win2.distribution_sku_name) check_num  from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1   left join ( select distribution_id,distribution_sku_name,valid_status,quantity,valid_quantity  from  dw_detail GROUP BY distribution_id,distribution_sku_name,valid_status,quantity,valid_quantity , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS win2   on  win2.distribution_id = win1.id  where win1.status = '3'  and  win1.catering_id IN ( select restaurant_id from  dim_school where organ_no is not null )  and win1.id NOT IN (select id from distributionDelete ) ) check_num ,  \n" +
                "         ( select count(distinct win1.id) accept_num  from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1    where win1.status = '3'  and  win1.catering_id IN ( select restaurant_id from  dim_school where organ_no is not null )   ) acceptnum ,  \n" +
                //  启动单数 startnum
                //"         ( select count(distinct win2.id) totalNum  from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1   left join ( select id,distribution_id,distribution_sku_name,valid_status,quantity,valid_quantity  from  dw_detail GROUP BY id,distribution_id,distribution_sku_name,valid_status,quantity,valid_quantity , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS win2   on  win2.distribution_id = win1.id  where win1.status in ('2','3')  and  win1.catering_id IN ( select restaurant_id from  dim_school )  and win1.id NOT IN (select id from distributionDelete )   ) totalNum   ,\n" +
                "         ( select count(distinct win1.id) start_num  from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1    where win1.status  in ('2','3')  and  win1.catering_id IN ( select restaurant_id from  dim_school where organ_no is not null )   ) startnum  , \n" +
                //  合格数 pass_num
                "         ( select count(distinct win2.distribution_sku_name) pass_num  from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1   left join ( select distribution_id,distribution_sku_name,valid_result,valid_quantity,distribution_quantity  from  dw_detail GROUP BY distribution_id,distribution_sku_name,valid_result,valid_quantity,distribution_quantity , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS win2   on  win2.distribution_id = win1.id  where win1.status = '3' and win2.valid_result = '1' and  win1.catering_id IN ( select restaurant_id from  dim_school where organ_no is not null )  ) pass_num   , \n" +
                //  验收数 check_num
                "         ( select count(distinct win2.distribution_sku_name) check_num  from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1   left join ( select distribution_id,distribution_sku_name,valid_result,valid_quantity,distribution_quantity  from  dw_detail GROUP BY distribution_id,distribution_sku_name,valid_result,valid_quantity,distribution_quantity , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS win2   on  win2.distribution_id = win1.id  where win1.status = '3'  and  win1.catering_id IN ( select restaurant_id from  dim_school where organ_no is not null )   ) check_num   , \n" +
                //  合格且达标 quantity_pass_num
                //"         ( select count(distinct win2.distribution_sku_name) quantity_pass_num  from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1   left join ( select distribution_id,distribution_sku_name,valid_status,quantity,valid_quantity  from  dw_detail GROUP BY distribution_id,distribution_sku_name,valid_status,quantity,valid_quantity , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS win2   on  win2.distribution_id = win1.id  where win1.status = '3' and win2.valid_status = '1' and win2.quantity = win2.valid_quantity and  win1.catering_id IN ( select restaurant_id from  dim_school where organ_no is not null )  and win1.id NOT IN (select id from distributionDelete )  )quantity_pass_num    \n"
                "         ( select count(distinct win2.distribution_sku_name) quantity_pass_num  from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1   left join ( select distribution_id,distribution_sku_name,valid_result,valid_quantity,distribution_quantity  from  dw_detail GROUP BY distribution_id,distribution_sku_name,valid_result,valid_quantity,distribution_quantity , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS win2   on  win2.distribution_id = win1.id  where win1.status = '3' and win2.valid_result = '1' and win2.valid_quantity >= win2.distribution_quantity and  win1.catering_id IN ( select restaurant_id from  dim_school where organ_no is not null )    )quantity_pass_num    \n"
        );



        //tenv.toRetractStream(finalTop,Row.class).print();
        //tenv.toChangelogStream(finalTop).print();

        //tenv.toAppendStream(finalTop,Row.class).print();
        //tenv.toDataStream(finalTop).print();


        // win test
        //Table test = tenv.sqlQuery(" select count(distinct win1.id)  from  ( select id,catering_id,status,TUMBLE_START(eventTime,INTERVAL '1' DAY ) as start_time , TUMBLE_END(eventTime,INTERVAL '1' DAY ) as end_time     from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1    where win1.status = '3'  and  win1.catering_id IN ( select restaurant_id from  dim_school where organ_no is not null )    ");
        //Table test = tenv.sqlQuery(" select count(distinct win1.id),start_time,end_time    from  ( select id,catering_id,status,TUMBLE_START(eventTime,INTERVAL '1' DAY ) as start_time , TUMBLE_END(eventTime,INTERVAL '1' DAY ) as end_time     from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1    where win1.status = '3'  and  win1.catering_id IN ( select restaurant_id from  dim_school where organ_no is not null ) group by start_time, end_time   ");


        //tenv.toRetractStream(test,Row.class).print();






        // mark 3.2 表（row）转dto
        DataStream<Tuple2<Boolean, TopTableDTO>> tuple2DataStream = tenv.toRetractStream(finalTop, TopTableDTO.class);
        //tuple2DataStream.print();


        SingleOutputStreamOperator<TopTableDTO> map2 = tuple2DataStream.filter(new FilterFunction<Tuple2<Boolean, TopTableDTO>>() {
            @Override
            public boolean filter(Tuple2<Boolean, TopTableDTO> value) throws Exception {
                //if (value.f0 == true && booleanMorningCheckTopTDTOTuple2.f1.okRate <= 100.0 && booleanMorningCheckTopTDTOTuple2.f1.mcRate <= 100.0 ) {
                if (! value.f0 ) {
                    return false;
                } else {
                    return true;
                }
            }
        })


            // 有可能导致最终结果不准确
                .filter(new FilterFunction<Tuple2<Boolean, TopTableDTO>>() {
            @Override
            public boolean filter(Tuple2<Boolean, TopTableDTO> val) throws Exception {

                if ((val.f1.executeRate > 100.0) || (val.f1.executeRate < 0.0)){
                    return false;
                }else {
                    return true;
                }

            }
        }).filter(new FilterFunction<Tuple2<Boolean, TopTableDTO>>() {
            @Override
            public boolean filter(Tuple2<Boolean, TopTableDTO> val) throws Exception {
                if  ((val.f1.qualifiedRate > 100.0) || (val.f1.qualifiedRate < 0.0)){
                    return false;
                }else {
                    return true;
                }
            }
        }).filter(new FilterFunction<Tuple2<Boolean, TopTableDTO>>() {
            @Override
            public boolean filter(Tuple2<Boolean, TopTableDTO> val) throws Exception {
                if  ((val.f1.notStandardRate > 100.0) || (val.f1.notStandardRate < 0.0)){
                    return false;
                }else {
                    return true;
                }
            }
        }).map(new MapFunction<Tuple2<Boolean, TopTableDTO>, TopTableDTO>() {
            @Override
            public TopTableDTO map(Tuple2<Boolean, TopTableDTO> booleanTopTableDTOTuple2) throws Exception {
                return booleanTopTableDTOTuple2.f1;
            }
        });


        //map2.print();

        SingleOutputStreamOperator<String> map3 = map2.map(new MapFunction<TopTableDTO, TopVO>() {
            @Override
            public TopVO map(TopTableDTO topTableDTO) throws Exception {
                return new TopVO(topTableDTO.executeRate, topTableDTO.qualifiedRate, topTableDTO.notStandardRate);
            }
        }).map(new MapFunction<TopVO, String>() {
            @Override
            public String map(TopVO topVO) throws Exception {
                return topVO.toString();
            }
        });





        //  mark 4.1      list


        Table finalList = tenv.sqlQuery(" select " +
                "startnum.organ_no AS schId  ,\n" +
                "startnum.organ_name AS schName  ,\n" +
                //"COALESCE(schCheckNum.check_num,0)  , \n" +
                //"COALESCE(schTotalNum.totalNum ,1)  ,\n" +
                //"schPassNum.pass_num  , \n" +
                //"schQuantityPassNum.quantity_pass_num  , \n" +
                //  1 executeRate
                "if(  COALESCE(startnum.start_num,0) = 0 , 0.0,  ROUND((CAST(COALESCE(acceptnum.accept_num,0) AS DOUBLE)/ COALESCE(startnum.start_num,0) *100 ) ,2) )        AS executeRate , \n" +
                //  2 qualifiedRate
                "if ( COALESCE(schCheckNum.check_num,0) = 0 , 0.0,  ROUND((CAST(COALESCE(schPassNum.pass_num,0) AS DOUBLE)/schCheckNum.check_num *100 ) ,2) )     AS qualifiedRate , \n" +
                //  3 notStandardRate
                "COALESCE(ROUND((CAST(CAST(schPassNum.pass_num AS INTEGER ) -  CAST(schQuantityPassNum.quantity_pass_num AS INTEGER ) AS DOUBLE)/schPassNum.pass_num *100 ) ,2),0)    AS notStandardRate , \n" +
                "CAST( CURRENT_TIMESTAMP as timestamp(3) )  as  rowTime \n" +
                "from \n" +
                // 启动单数 startnum (包含学校最多为主视图)
                "(select  COUNT(distinct win.id)  AS start_num ,organ_no,organ_name  from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS win     left join  dim_school ON  dim_school.restaurant_id = win.catering_id    where    dim_school.organ_no is not null and  win.status in ('2','3')  and  win.catering_id IN ( select restaurant_id from  dim_school) group by organ_no , organ_name  )  startnum   \n" +
                // left join 验收单数 acceptnum （join不到可能为null）
                "left join  (select  COUNT(distinct win.id) AS accept_num ,organ_no,organ_name   from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win    left join  dim_school ON  dim_school.restaurant_id = win.catering_id    where dim_school.organ_no is not null and win.status = '3'  and  win.catering_id IN ( select restaurant_id from  dim_school)  group by organ_no , organ_name) acceptnum  \n" +
                "on acceptnum.organ_no = startnum.organ_no \n" +
                // left join 合格数 pass_num
                "left join (select count(distinct win2.distribution_sku_name) pass_num  ,organ_no,organ_name  from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1   left join ( select id,distribution_id,distribution_sku_name,valid_result,valid_quantity,distribution_quantity  from  dw_detail GROUP BY id, distribution_id,distribution_sku_name,valid_result,valid_quantity,distribution_quantity , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS win2   on  win2.distribution_id = win1.id    left join  dim_school ON  dim_school.restaurant_id = win1.catering_id    where dim_school.organ_no is not null and  win1.status = '3' and win2.valid_result = '1' and  win1.catering_id IN ( select restaurant_id from  dim_school)  group by organ_no , organ_name) schPassNum \n" +
                "on schPassNum.organ_no = startnum.organ_no \n" +
                // left join 验收数 check_num
                "left join  (select  count(distinct win2.distribution_sku_name)  check_num ,organ_no,organ_name   from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1   left join ( select id, distribution_id,distribution_sku_name,valid_result,valid_quantity,distribution_quantity  from  dw_detail GROUP BY id, distribution_id,distribution_sku_name,valid_result,valid_quantity,distribution_quantity , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS win2   on  win2.distribution_id = win1.id     left join  dim_school ON  dim_school.restaurant_id = win1.catering_id    where dim_school.organ_no is not null and win1.status = '3'  and  win1.catering_id IN ( select restaurant_id from  dim_school)  group by organ_no , organ_name) schCheckNum  \n" +
                "on schCheckNum.organ_no = startnum.organ_no \n"+
                // left join 合格且达标 quantity_pass_num
                "left join (select count(distinct win2.distribution_sku_name) quantity_pass_num ,organ_no,organ_name  from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1   left join ( select id, distribution_id,distribution_sku_name,valid_result,valid_quantity,distribution_quantity  from  dw_detail GROUP BY id, distribution_id,distribution_sku_name,valid_result,valid_quantity,distribution_quantity , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS win2   on  win2.distribution_id = win1.id    left join  dim_school ON  dim_school.restaurant_id = win1.catering_id     where dim_school.organ_no is not null and win1.status = '3' and win2.valid_result = '1' and win2.valid_quantity >= win2.distribution_quantity and  win1.catering_id IN ( select restaurant_id from  dim_school)  group by organ_no , organ_name    ) schQuantityPassNum \n" +
                "on schQuantityPassNum.organ_no = startnum.organ_no \n"

                //"(select  COUNT(distinct win2.id)  AS totalNum ,organ_no,organ_name  from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1   left join ( select id,distribution_id,distribution_sku_name,valid_status,quantity,valid_quantity  from  dw_detail GROUP BY id,distribution_id,distribution_sku_name,valid_status,quantity,valid_quantity , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS win2   on  win2.distribution_id = win1.id    left join  dim_school ON  dim_school.restaurant_id = win1.catering_id    where    dim_school.organ_no is not null and  win1.status in ('2','3')  and  win1.catering_id IN ( select restaurant_id from  dim_school) group by organ_no , organ_name  )  schTotalNum   \n" +
                //"left join  (select  count(distinct win2.distribution_sku_name)  check_num ,organ_no,organ_name   from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1   left join ( select distribution_id,distribution_sku_name,valid_status,quantity,valid_quantity  from  dw_detail GROUP BY distribution_id,distribution_sku_name,valid_status,quantity,valid_quantity , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS win2   on  win2.distribution_id = win1.id     left join  dim_school ON  dim_school.restaurant_id = win1.catering_id    where dim_school.organ_no is not null and win1.status = '3'  and  win1.catering_id IN ( select restaurant_id from  dim_school) and win1.id NOT IN (select id from distributionDelete ) group by organ_no , organ_name) schCheckNum  \n" +
                //"on schCheckNum.organ_no = schTotalNum.organ_no \n"+
                //"left join (select count(distinct win2.distribution_sku_name) pass_num  ,organ_no,organ_name  from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1   left join ( select distribution_id,distribution_sku_name,valid_status,quantity,valid_quantity  from  dw_detail GROUP BY distribution_id,distribution_sku_name,valid_status,quantity,valid_quantity , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS win2   on  win2.distribution_id = win1.id    left join  dim_school ON  dim_school.restaurant_id = win1.catering_id    where dim_school.organ_no is not null and  win1.status = '3' and win2.valid_status = '1' and  win1.catering_id IN ( select restaurant_id from  dim_school) and win1.id NOT IN (select id from distributionDelete )  group by organ_no , organ_name) schPassNum \n" +
                //"on schPassNum.organ_no = schTotalNum.organ_no \n" +
                //"left join (select count(distinct win2.distribution_sku_name) quantity_pass_num ,organ_no,organ_name  from  ( select id,catering_id,status  from  dw_distribution GROUP BY id,catering_id,status , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS  win1   left join ( select distribution_id,distribution_sku_name,valid_status,quantity,valid_quantity  from  dw_detail GROUP BY distribution_id,distribution_sku_name,valid_status,quantity,valid_quantity , TUMBLE(eventTime, INTERVAL '1' DAY) ) AS win2   on  win2.distribution_id = win1.id    left join  dim_school ON  dim_school.restaurant_id = win1.catering_id     where dim_school.organ_no is not null and win1.status = '3' and win2.valid_status = '1' and win2.quantity = win2.valid_quantity and  win1.catering_id IN ( select restaurant_id from  dim_school) and win1.id NOT IN (select id from distributionDelete ) group by organ_no , organ_name    ) schQuantityPassNum \n" +
                //"on schQuantityPassNum.organ_no = schTotalNum.organ_no \n"
        );


        //tenv.toRetractStream(finalList,Row.class).print();





        DataStream<Tuple2<Boolean, ListTableDTO>> tuple2DataStream1 = tenv.toRetractStream(finalList, ListTableDTO.class);

        //tuple2DataStream1.print();

        SingleOutputStreamOperator<ListVO> map5 = tuple2DataStream1.filter(new FilterFunction<Tuple2<Boolean, ListTableDTO>>() {
            @Override
            public boolean filter(Tuple2<Boolean, ListTableDTO> booleanListTableDTOTuple2) throws Exception {
                if ( (!booleanListTableDTOTuple2.f0)  || (booleanListTableDTOTuple2.f1.schId == null) ) {
                    return false;
                } else {
                    return true;
                }
            }
        })
                // 有可能导致最终结果不准确
                .filter(new FilterFunction<Tuple2<Boolean, ListTableDTO>>() {
            @Override
            public boolean filter(Tuple2<Boolean, ListTableDTO> val) throws Exception {

                if ( (val.f1.executeRate > 100.0) || (val.f1.executeRate < 0.0)){
                    return false;
                }else {
                    return true;
                }

            }
        }).filter(new FilterFunction<Tuple2<Boolean, ListTableDTO>>() {
            @Override
            public boolean filter(Tuple2<Boolean, ListTableDTO> val) throws Exception {
                if ((val.f1.qualifiedRate > 100.0) || (val.f1.qualifiedRate < 0.0)){
                    return false;
                }else {
                    return true;
                }
            }
        }).filter(new FilterFunction<Tuple2<Boolean, ListTableDTO>>() {
            @Override
            public boolean filter(Tuple2<Boolean, ListTableDTO> val) throws Exception {
                if ((val.f1.notStandardRate > 100.0) || (val.f1.notStandardRate < 0.0)){
                    return false;
                }else {
                    return true;
                }
            }
        }).map(new MapFunction<Tuple2<Boolean, ListTableDTO>, ListVO>() {
            @Override
            public ListVO map(Tuple2<Boolean, ListTableDTO> val) throws Exception {
                return new ListVO(val.f1.schId, val.f1.schName, val.f1.executeRate, val.f1.qualifiedRate, val.f1.notStandardRate,val.f1.rowTime);
            }
        });

        SingleOutputStreamOperator<String> stringList = map5.map(new MapFunction<ListVO, String>() {
            @Override
            public String map(ListVO listVO) throws Exception {
                return listVO.toString();
            }
        });


        //  mark    list all


        SingleOutputStreamOperator<ListVO> listVOSingleOutputStreamOperator = map5.assignTimestampsAndWatermarks(WatermarkStrategy.<ListVO>noWatermarks()
                .withTimestampAssigner(new SerializableTimestampAssigner<ListVO>() {
                    @Override
                    public long extractTimestamp(ListVO listVO, long l) {
                        return listVO.getRowTime().getTime();
                    }
                }));
        //map5.print();

        //listVOSingleOutputStreamOperator.print();

        SingleOutputStreamOperator<JSONArray> apply = listVOSingleOutputStreamOperator.windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(new OneByOneTrigger())
                .apply(new AllWindowFunction<ListVO, JSONArray, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<ListVO> iterable, Collector<JSONArray> collector) throws Exception {

                        //    1 新建 dto
                        HashMap<String, ListAllVO> hashMap = new HashMap<>();
                        for (ListVO listVO : iterable) {
                            ListAllVO listAllVO = new ListAllVO(listVO.getSchId(), listVO.getSchName(), listVO.getExecuteRate(), listVO.getQualifiedRate(), listVO.getNotStandardRate());
                            hashMap.put(listVO.getSchId(), listAllVO);
                        }
                        //    2 传JSONArray
                        JSONArray jsonArray = new JSONArray();
                        for (Map.Entry<String, ListAllVO> entry : hashMap.entrySet()) {
                            JSONObject jsonObject = new JSONObject();
                            jsonObject.put("schId", entry.getValue().getSchId());
                            jsonObject.put("schName", entry.getValue().getSchName());
                            jsonObject.put("executeRate", entry.getValue().getExecuteRate());
                            jsonObject.put("qualifiedRate", entry.getValue().getQualifiedRate());
                            jsonObject.put("notStandardRate", entry.getValue().getNotStandardRate());
                            jsonArray.add(jsonObject);
                            //collector.collect(jsonArray);
                        }
                        collector.collect(jsonArray);

                    }
                });

        //map3.print();
        //stringList.print();
        //apply.print();

        map3.addSink(KafkaIO.getTopSink()).name(Kafka.ADS_ORD_TOP);
        stringList.addSink(KafkaIO.getListSink()).name(Kafka.ADS_ORD_LIST_CDC);
        apply.addSink(KafkaIO.getListAllSink()).name(Kafka.ADS_ORD_LIST_ALL);

        env.execute("验收-0905");


    }




}
