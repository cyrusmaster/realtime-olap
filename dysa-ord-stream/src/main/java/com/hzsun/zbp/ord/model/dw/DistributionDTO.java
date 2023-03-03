package com.hzsun.zbp.ord.model.dw;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class DistributionDTO {


    private String id;

    private String cateringId;

    private String status;

    private String accept_time;

    private String create_time;


    public DistributionDTO(ObjectNode jsonNodes) {
        JsonNode afterData  =  jsonNodes.get("after");

        String id = afterData.get("id").toString().replace("\"", "");
        this.id = id;

        String catering_id = afterData.get("catering_id").toString().replace("\"", "");
        this.cateringId = catering_id;

        String status = afterData.get("status").toString().replace("\"", "");
        this.status = status;

        String accept_time = afterData.get("accept_time").toString().replace("\"", "");
        this.accept_time = accept_time;

        String create_time = afterData.get("create_time").toString().replace("\"", "");
        this.create_time = create_time;
    }


    @Override
    public String toString() {
        return "DistributionDTO{" +
                "id='" + id + '\'' +
                ", cateringId='" + cateringId + '\'' +
                ", status='" + status + '\'' +
                ", accept_time='" + accept_time + '\'' +
                ", create_time='" + create_time + '\'' +
                '}';
    }


    public String getId() {
        return id;
    }

    public String getCateringId() {
        return cateringId;
    }

    public String getStatus() {
        return status;
    }

    public String getAccept_time() {
        return accept_time;
    }

    public String getCreate_time() {
        return create_time;
    }


    public void setId(String id) {
        this.id = id;
    }
}
