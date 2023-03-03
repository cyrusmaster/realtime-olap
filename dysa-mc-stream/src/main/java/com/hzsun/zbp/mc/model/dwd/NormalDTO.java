package com.hzsun.zbp.mc.model.dwd;


import com.hzsun.zbp.mc.utils.TimeUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.sql.Timestamp;


//接收 binlog 清洗dw 表
public class NormalDTO {


    //  ID
    private String id;

    //  餐饮单位ID
    private String cateringId;

    //  员工ID
    private String safeterId;

    //  设备编号
    private String deviceNo;

    //  检测时间
    private Long signTime;

    // 检测类型(1:上班;0:下班)
    private String signType;

    // 温度
    private String temperature;

//            "id": "0065725da981fb6475b3119e20220328",
//                    "catering_id": "8affe5630d65d2b5d3af5685a8745efb",
//                    "safeter_id": "0944e8be036541adb426b566799b1050",
//                    "device_no": "3GKL2HBZG3",
//                    "sign_time": 1648536576000,
//                    "sign_type": "1",
//                    "temperature": "23",
//                    "face": null


    public NormalDTO(JsonNode data) {
        JsonNode afterData  =  data.get("after");

        this.id = afterData.get("id").toString().replace("\"", "");
        this.cateringId = afterData.get("catering_id").toString().replace("\"", "");
        this.safeterId = afterData.get("safeter_id").toString().replace("\"", "");
        this.deviceNo = afterData.get("device_no").toString().replace("\"", "");
        // 0609 处理datetime类型时间  时间 -8
        //this.signTime = afterData.get("sign_time").toString();
        String sign_time = afterData.get("sign_time").toString();
        this.signTime = TimeUtils.getTrueTimestamp(sign_time);

        this.signType = afterData.get("sign_type").toString().replace("\"", "");
        this.temperature = afterData.get("temperature").isNull() ? "" : afterData.get("temperature").toString().replace("\"", "");
    }


    @Override
    public String toString() {
        return id +","+ cateringId+","+ safeterId+","+ deviceNo+","+ signTime+","+ signType +","+ temperature ;
    }


}
