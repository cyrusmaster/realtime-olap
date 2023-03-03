package com.hzsun.zbp.mc.model.dwd;

import com.alibaba.fastjson.JSONObject;
import com.hzsun.zbp.mc.config.KafkaConfig;
import com.hzsun.zbp.mc.model.table.MapAakTO;
import com.hzsun.zbp.mc.utils.TimeUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


// 接收 binlog 清洗dw 表
public class AbnormalDTO {

    //ID
    private String id;
    //餐饮单位ID
    private String cateringId;
    //员工ID
    private String safeterId;
    //设备编号
    private String deviceNo;
    //检测时间
    private Long signTime;
    //异常类型
    private String type;
    //温度
    private String temperature;
    //手心照片
    private String handPhoto;
    //手背照片
    //private String bhand;
    //异常描述
    private String description;
    //手心异常详情
    private String handInfo;
    //手背异常详情
    //private String bhandInfo;
    //问询异常详情
    private String enquiryItem;
    //审核
    private String audit;
    //审核信息
    private String auditMessage;


    public AbnormalDTO(JsonNode data ) {

        JsonNode afterData  =  data.get("after");

        this.id = afterData.get("id").toString().replace("\"", "");
        this.cateringId = afterData.get("catering_id").toString().replace("\"", "");
        this.safeterId = afterData.get("safeter_id").toString().replace("\"", "");
        this.deviceNo = afterData.get("device_no").toString().replace("\"", "");

        //this.signTime = afterData.get("sign_time").toString();
        this.signTime = TimeUtils.getTrueTimestamp(afterData.get("sign_time").toString());


        this.type =afterData.get("type").toString().replace("\"", "");
        this.temperature = afterData.get("temperature").toString().replace("\"", "");

        // 异常图片
        //this.fhand = afterData.get("fhand").isNull() ? "" : afterData.get("fhand").toString().replace("\"", "") ;
        //this.bhand = afterData.get("bhand").isNull() ? "" : afterData.get("bhand").toString().replace("\"", "") ;
        String fhand = afterData.get("fhand").isNull() ? "" : afterData.get("fhand").toString().replace("\"", "") ;
        String bhand = afterData.get("bhand").isNull() ? "" : afterData.get("bhand").toString().replace("\"", "") ;
        this.handPhoto = fhand + "|" + bhand;

        this.description = afterData.get("description").isNull() ? "" : afterData.get("description").toString().replace("\"", "") ;
        //        处理不了 字段有 ， 分割报错
        //        this.fhandInfo = afterData.get("fhand_info").isNull() ? "" : afterData.get("fhand_info").toString().replace("\"", "") ;
        //        this.bhandInfo = afterData.get("bhand_info").isNull() ? "" : afterData.get("bhand_info").toString().replace("\"", "") ;
        String fhandInfo = afterData.get("fhand_info").isNull() ? "" : afterData.get("fhand_info").toString().substring(1,afterData.get("fhand_info").toString().length()-1).replace("\\", "");
        String bhandInfo = afterData.get("bhand_info").isNull() ? "" : afterData.get("bhand_info").toString().substring(1,afterData.get("bhand_info").toString().length()-1).replace("\\", "");

        String out1 = "";
        String out2 = "";
        // 0 维度写死
        HashMap<String, String> handError = new HashMap();
        handError.put("Wound", "创伤");
        handError.put("NailColor", "指甲油");
        handError.put("LongNail", "长指甲");
        handError.put("GrayNail", "灰指甲");
        handError.put("Plaster", "创口贴");
        handError.put("Plaster1", "横向创口贴");
        handError.put("Plaster2", "纵向创口贴");
        handError.put("Glove", "手套");
        handError.put("Chilblain", "冻疮");
        handError.put("Breacelet", "手镯手链");
        handError.put("Watch", "手表");
        handError.put("Ring", "戒指");



        // 2 遍历映射
        if ( !("".equals(fhandInfo))) {

            //    1 JSONObject
            JSONObject jsonObject = JSONObject.parseObject(fhandInfo);
            //  2 foreach keyset
            for (String key : jsonObject.keySet()) {
                String o = handError.get(key);
                out1 += o;
            }
            out1 = "手心:" + out1;
        }

        if ( !("".equals(bhandInfo))) {

            JSONObject jsonObject = JSONObject.parseObject(bhandInfo);
            for (String key : jsonObject.keySet()){
                String s = handError.get(key);
                out2 += s;
            }
            out2 = "手背:" + out2;
        }

        this.handInfo = out1+" "+out2;

         String enq = afterData.get("enquiry_item").isNull() ? "" : afterData.get("enquiry_item").toString().substring(1,afterData.get("enquiry_item").toString().length()-1).replace("\\", "") ;
        //this.enquiryItem = afterData.get("enquiry_item").isNull() ? "" : "" ;

        String out3 = "";

        // 0 维度写死
        HashMap<String, String> askError = new HashMap();
        askError.put("61e1c6b647a4560254d7ed9abcb6e88d", "活动性肺结核");
        askError.put("6513c7c4b807bd990dcd285393ccba15", "腹泻");
        askError.put("6941c56881c24965e81152781999d81b", "发热");
        askError.put("70b84b5ec3ba075c2f259d95a9af71ea", "眼花");
        askError.put("74514cde0a3e9007c3011084a8a6f457", "伤寒");
        askError.put("b091e5fb20148cc5a79490a76c31d607", "黄疸");
        askError.put("b8581b96f2fbd2be14f0991921cf0210", "头痛");
        askError.put("d1f22381c50d93c58e800d4e8e3e0ff0", "化脓、渗出性皮肤病");
        askError.put("00f7432cd779adeb450514038ac2d702", "发热");
        askError.put("066500142f4d92cc9ebe1ddd74f8cbed", "皮疹");
        askError.put("0f1f9324d527299ddbac1a07c73f795b", "发热");
        askError.put("1bbe626987f7df2fbb3d4c49a7469fb5", "呕吐");
        askError.put("2c30dc54686178450ac1a4f5d56a4035", "感冒");
        askError.put("2ea53850a8f6ed4eb8c930771c551866", "痢疾");
        askError.put("4334c631eaab5171a1f83587a97c6041", "发热");
        askError.put("502bd865dbe1184abd73e8bf721fc184", "低烧");
        askError.put("52ae2e2fd8d668c1de4835d3d5a39279", "病毒性肝炎");

        if ( !("".equals(enq))) {
            //    1 JSONObject
            JSONObject jsonObject = JSONObject.parseObject(enq);
            //  2 foreach keyset
            for (String key : jsonObject.keySet()) {
                String o = askError.get(key)+" ";
                out3 += o ;
            }

        }

        this.enquiryItem = out3;

        this.audit = afterData.get("audit").isNull() ? "" : afterData.get("audit").toString().replace("\"", "") ;

        //this.auditMessage = afterData.get("audit_message").isNull() ? "" : afterData.get("audit_message").toString().replace("\"", "") ;
        this.auditMessage = afterData.get("audit_message").isNull() ? "" : "" ;
    }

//     不写 无法调用tostring
    @Override
    public String toString() {
        return id +","+ cateringId +","+  safeterId +","+  deviceNo +","+ signTime +","+ type +","+ temperature +","+  handPhoto +","+  description +","+  handInfo +"," + enquiryItem +","+ audit +","+ auditMessage;
    }




    public static void main(String[] args)throws Exception {



        // mark  实践从hive 读取失败    会造成任务一直执行


        //String enq = afterData.get("enquiry_item").isNull() ? "" : afterData.get("enquiry_item").toString().substring(1,afterData.get("enquiry_item").toString().length()-1).replace("\\", "") ;

        //String enq  = "{\"502bd865dbe1184abd73e8bf721fc184\": 1}";
        //
        //
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //
        //
        ////tenv.executeSql("drop table dim_ask_item");
        //tenv.executeSql("CREATE TABLE  if not exists dim_ask_item   (" +
        //        "  enquiry_id  string  ," +
        //        "  enquiry_title  string  " +
        //        //"  PRIMARY KEY (enquiry_id) NOT ENFORCED " +
        //        ") " +
        //        "WITH (" +
        //        "    'connector' = 'kafka',\n" +
        //        "    'topic' = '"+ KafkaConfig.ASK_ITEM +"',\n" +
        //        "    'properties.bootstrap.servers' = '"+ KafkaConfig.KAFKA_BROKER_LIST +"'  ,\n" +
        //        //"    'format' = 'csv',  \n"
        //        "    'format' = 'csv')"
        //);
        //
        //
        //Table table = tenv.sqlQuery(" SELECT  enquiry_id ,enquiry_title   FROM dim_ask_item");
        //
        //DataStream<MapAakTO> mapAakTODataStream = tenv.toAppendStream(table, MapAakTO.class);
        ////mapAakTODataStream.print();
        //
        //
        //DataStream<Tuple2<Boolean, MapAakTO>> tuple2DataStream = tenv.toRetractStream(table,MapAakTO.class);
        ////tuple2DataStream.print();
        //
        //tuple2DataStream.map(new MapFunction<Tuple2<Boolean, MapAakTO>, Map<String,String>>() {
        //    @Override
        //    public Map<String, String> map(Tuple2<Boolean, MapAakTO> booleanMapAakTOTuple2) throws Exception {
        //        Map<String, String> map = new HashMap<>();
        //        map.put(booleanMapAakTOTuple2.f1.enquiry_id,booleanMapAakTOTuple2.f1.enquiry_title);
        //
        //        return map;
        //    }
        //}).filter(new FilterFunction<Map<String, String>>() {
        //    @Override
        //    public boolean filter(Map<String, String> stringStringMap) throws Exception {
        //
        //        JSONObject jsonObject = JSONObject.parseObject(enq);
        //        //  2 foreach keyset
        //        for (String key : jsonObject.keySet()) {
        //
        //            Set<String> strings = stringStringMap.keySet();
        //            if (strings.contains(key)){
        //                return true;
        //            }
        //        }
        //
        //        return false;
        //    }
        //}).print();


        //tuple2DataStream.map(new MapFunction<Tuple2<Boolean, Row>, Map<String,String>>() {
        //    @Override
        //    public Map<String, String> map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
        //
        //        Map<String, String> map = new HashMap<>();
        //
        //        map.put(booleanRowTuple2.f1)
        //
        //
        //
        //
        //        return null;
        //    }
        //});



        //env.execute();

    }


}
