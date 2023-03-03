package com.hzsun.zbp.ord.model.dw;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

 /**
  * REMARK   明细表  
  * @className   DetailDTO
  * @date  2022/6/16 15:52
  * @author  cyf  
  */ 
public class DetailDTO {


    private String id;

    private String distributionId;

    private String distributionSkuName;

    private String validResult;

    // 弃用
    private String quantity;

    private String validQuantity;

    //新增判断字段
    private String distributionQuantity;


    public DetailDTO(ObjectNode jsonNodes) {

        JsonNode afterData  =  jsonNodes.get("after");

        this.id = afterData.get("id").toString().replace("\"", "");

        this.distributionId = afterData.get("distribution_id").toString().replace("\"", "");

        this.distributionSkuName = afterData.get("distribution_sku_name").toString().replace("\"", "");

        this.validResult = afterData.get("valid_result").toString().replace("\"", "");

        this.quantity = afterData.get("quantity").toString().replace("\"", "");

        this.validQuantity = afterData.get("valid_quantity").toString().replace("\"", "");

        this.distributionQuantity = afterData.get("distribution_quantity").toString().replace("\"", "");



    }


    @Override
    public String toString() {
        return "DetailDTO{" +
                "distributionId='" + distributionId + '\'' +
                ", distributionSkuName='" + distributionSkuName + '\'' +
                ", validResult='" + validResult + '\'' +
                ", quantity='" + quantity + '\'' +
                ", validQuantity='" + validQuantity + '\'' +
                '}';
    }


    public String getId() {
        return id;
    }

    public String getDistributionId() {
        return distributionId;
    }

    public String getDistributionSkuName() {
        return distributionSkuName;
    }

    public String getValidResult() {
        return validResult;
    }

    public String getQuantity() {
        return quantity;
    }

    public String getValidQuantity() {
        return validQuantity;
    }

    public String getDistributionQuantity(){return distributionQuantity; }
}
