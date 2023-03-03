package com.hzsun.zbp.iot.model.dwd;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.Timestamp;

public class SamplePrintCdcDTO {



    private String cateringId;

    private String sampleId;

    private String foodId;

    private String status;

    private Long createDate;



    public SamplePrintCdcDTO(ObjectNode jsonNodes) {

        JsonNode afterData  =  jsonNodes.get("after").get("catering_id");
        String s = afterData.toString().replace("\"", "");
        this.cateringId = s;

        JsonNode afterData2  =  jsonNodes.get("after").get("sample_id");
        String s2 = afterData2.toString().replace("\"", "");
        this.sampleId = s2;


        JsonNode jsonNode = jsonNodes.get("after").get("food_id");
        String replace = jsonNode.toString().replace("\"", "");
        this.foodId = replace;


        JsonNode jsonNode2 = jsonNodes.get("after").get("status");
        String replace2 = jsonNode2.toString().replace("\"", "");
        this.status = replace2;


        JsonNode jsonNode3 = jsonNodes.get("after").get("create_date");
        String create_date =   jsonNode3.toString().replace("\"", "");


        Long before = Long.valueOf(create_date);
        Long eight = 8 * 60 * 60 * 1000L;
        Long after = before - eight;


        this.createDate = after;



    }


    public String getCateringId() {
        return cateringId;
    }

    public String getSampleId() {
        return sampleId;
    }

    public String getFoodId() {
        return foodId;
    }

    public String getStatus() {
        return status;
    }

    public Long getCreateDate() {
        return createDate;
    }

    @Override
    public String toString() {
        return "{" +
                cateringId +
                "," + sampleId +
                "," + foodId +
                "," + status +
                "," + createDate +
                "}";
    }
}
