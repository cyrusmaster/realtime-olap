package com.hzsun.zbp.mc.filter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;

public class Filter {



    public static boolean normalToFilter(ObjectNode value){


        //1  空判断
        if (value.isEmpty()) {
            return false;
        }

        //2 只需要c操作 晨检也只有c操作
        String opdate = value.get("op").toString().replace("\"", "");
        if (!"c".equals(opdate)) {
            return false;
        }

        //3 after
        JsonNode datad = value.get("after");
        if (datad.isNull()) {
            return false;
        }
        //4 值判断  检测类型
        String sign_type = datad.get("sign_type").toString().replace("\"", "");
        if (!"1".equals(sign_type)){
            return false;
        }


        return true;

    }


    public static boolean abnormalToFilter(ObjectNode value){


        //1  空判断
        if (value.isEmpty()) {
            return false;
        }

        //2 只需要c操作 晨检也只有c操作
        String opdate = value.get("op").toString().replace("\"", "");
        if (!"c".equals(opdate)) {
            return false;
        }

        //3 after
        JsonNode datad = value.get("after");
        if (datad.isNull()) {
            return false;
        }
        //4 值判断



        return true;

    }

}
