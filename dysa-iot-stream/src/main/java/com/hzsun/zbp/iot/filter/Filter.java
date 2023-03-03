package com.hzsun.zbp.iot.filter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;

public class Filter {

    //        过滤非插入    c 插入  u 更新
    private static String[] NumArr = new String[]{"c","u"};





    //  print 表过滤
    public static boolean samplePrintFilter(ObjectNode value){

        if (value.isEmpty()) {
            return false;
        }



//        获取 debezium json 格式  判断  类型是否null
        JsonNode data = value.get("op");
        if (data.isNull()) {
            return false;
        }


        //  只有更新 有今天的数据  TODO 应该改成 插入
        String opdate = data.toString().replace("\"", "");
        if (! Arrays.asList(NumArr).contains(opdate)) {
            return false;
        }
        //if (! "u".contains(opdate)) {
        //    //System.out.println("3123");
        //    return false;
        //}



        //      获取 debezium json 格式  判断 数据是否null
        JsonNode datad = value.get("after");
        if (datad.isNull()) {
            return false;
        }


        // 对 iot 字段 的过滤
        JsonNode dataId = datad.get("catering_id");
        if (dataId.isNull()) {
            return false;
        }

        JsonNode sampleId = datad.get("sample_id");
        if (sampleId.isNull()) {
            return false;
        }

        JsonNode foodInfo = datad.get("food_id");
        if (foodInfo.isNull()) {
            return false;
        }

        // 过滤 非 取样完成
        String status = datad.get("status").toString().replace("\"", "");
        if (! "2".equals(status)) {
            return false;
        }
        //




        return true;

    }



    //  取样次数 和 取样达标次数
    public static boolean sampleFilter(ObjectNode value){

        if (value.isEmpty()) {
            return false;
        }


        JsonNode data = value.get("op");
        // 都要 计算时再过滤
        String opdate = data.toString().replace("\"", "");
        if (! Arrays.asList(NumArr).contains(opdate)) {
            return false;
        }


        //      获取 debezium json 格式  判断 数据是否null
        JsonNode datad = value.get("after");
        if (datad.isNull()) {
            return false;
        }


        // 对 iot 字段 的过滤
        //JsonNode dataId = datad.get("id");
        //if (dataId.isNull()) {
        //    return false;
        //}

        //JsonNode person_id = datad.get("person_id");
        //if (person_id.isNull()) {
        //    return false;
        //}


        //del_flag    过滤 非 0
        String del_flag = datad.get("del_flag").toString().replace("\"", "");
        if (! "0".equals(del_flag)) {
            return false;
        }

        String status = datad.get("status").toString().replace("\"", "");
        if (! "2".equals(status)) {
            return false;
        }








        //
        //JsonNode personIdInfo = datad.get("person_id");
        //if (personIdInfo.isNull()) {
        //    return false;
        //}

        //
        //JsonNode endDate = datad.get("person_id");
        //if (endDate.isNull()) {
        //    return false;
        //}



        return true;

    }


    public static boolean sampleFilter_c(ObjectNode value){

        if (value.isEmpty()) {
            return false;
        }


        JsonNode data = value.get("op");
        // 都要 计算时再过滤
        String opdate = data.toString().replace("\"", "");
        if (! Arrays.asList(NumArr).contains(opdate)) {
            return false;
        }


        //      获取 debezium json 格式  判断 数据是否null
        JsonNode datad = value.get("after");
        if (datad.isNull()) {
            return false;
        }


        // 对 iot 字段 的过滤
        //JsonNode dataId = datad.get("id");
        //if (dataId.isNull()) {
        //    return false;
        //}

        //JsonNode person_id = datad.get("person_id");
        //if (person_id.isNull()) {
        //    return false;
        //}


        //del_flag    过滤 非 0
        String del_flag = datad.get("del_flag").toString().replace("\"", "");
        if (! "0".equals(del_flag)) {
            return false;
        }

        String status = datad.get("status").toString().replace("\"", "");
        if (! "2".equals(status)) {
            return false;
        }



        //
        //JsonNode personIdInfo = datad.get("person_id");
        //if (personIdInfo.isNull()) {
        //    return false;
        //}

        //
        //JsonNode endDate = datad.get("person_id");
        //if (endDate.isNull()) {
        //    return false;
        //}



        return true;

    }





}
