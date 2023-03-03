package com.hzsun.zbp.ord.filter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;

public class Filter {

    //        过滤非插入    c 插入  u 更新
    private static String[] NumArr = new String[]{"c","u"};





    //  print 表过滤
    public static boolean detailFilter(ObjectNode value){

        if (value.isEmpty()) {
            return false;
        }

        // 去掉 全部统计  只统计更新
        String opdate =  value.get("op").toString().replace("\"", "");
        //if (! Arrays.asList(NumArr).contains(opdate)) {
        //    return false;
        //}

        if (! "u".contains(opdate)) {
            //System.out.println("3123");
            return false;
        }

        JsonNode datad = value.get("after");
        if (datad.isNull()) {
            return false;
        }

        //JsonNode id = datad.get("id");
        //if (id.isNull()) {
        //    return false;
        //}

        // 对 distribution_sku_name 字段 的过滤 (貌似只有更新记录有 )
        //JsonNode dataName = datad.get("distribution_sku_name");
        //if (dataName.isNull()) {
        //    return false;
        //}


        //JsonNode dataId = datad.get("distribution_id");
        //if (dataId.isNull()) {
        //    return false;
        //}


        // 过滤 非 取样完成
        //String status = datad.get("status").toString().replace("\"", "");
        //if (! "2".equals(status)) {
        //    return false;
        //}
        //


        return true;

    }



    public static boolean distributionFilter(ObjectNode value){

        if (value.isEmpty()) {
            return false;
        }


        String opdate = value.get("op").toString().replace("\"", "");
        //if (! Arrays.asList(NumArr).contains(opdate)) {
        //    return false;
        //}

        if (! "u".contains(opdate)) {
            return false;
        }


        JsonNode datad = value.get("after");
        if (datad.isNull()) {
            return false;
        }

        //JsonNode id = datad.get("id");
        //if (id.isNull()) {
        //    return false;
        //}
        //
        //
        //JsonNode catering_id = datad.get("catering_id");
        //if (catering_id.isNull()) {
        //    return false;
        //}


        return true;

    }


    // 删除  "op":"d"
    public static boolean distributionDeleteFilter(ObjectNode value){

        if (value.isEmpty()) {
            return false;
        }

        String opdate = value.get("op").toString().replace("\"", "");
        if (! "d".equals(opdate)) {
            return false;
        }

        return true;

    }





}
