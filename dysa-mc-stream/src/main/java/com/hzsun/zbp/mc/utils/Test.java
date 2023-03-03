package com.hzsun.zbp.mc.utils;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Test {

    public static void main(String[] args) {


        // json
        // {"LongNail": 1, "Breacelet": 1}
        //"{\"LongNail\":1}"

        String xin = "{\"LongNail\":1}";
        //String xin = null;
        String bei = "{\"LongNail\": 1, \"Breacelet\": 1}";


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


        if ( !(xin == null)) {

            //    1 JSONObject
            JSONObject jsonObject = JSONObject.parseObject(xin);
            //    2  for
            //for (int i =1 ; i <= jsonObject.size() ; i++){
            //
            //    //Set<Map.Entry<String, Object>> entries = jsonObject.entrySet();
            //
            //    jsonObject
            //
            //
            //
            //    System.out.println(strings);
            //
            //
            //}



            //  2 foreach keyset
            Set<String> strings = jsonObject.keySet();

            //System.out.println(strings);

            for (String key : strings) {

                String o = handError.get(key);

                out1 += o;


            }
            out1 = "手心:" + out1;

        }




         if ( !(bei == null)) {

            JSONObject jsonObject = JSONObject.parseObject(bei);
            for (String key : jsonObject.keySet()){
                String s = handError.get(key);
                out2 += s;
            }
            out2 = "手背:" + out2;

        }



        System.out.println(out1+" "+out2);



        //System.out.println(jsonObject.size());





    }


}
