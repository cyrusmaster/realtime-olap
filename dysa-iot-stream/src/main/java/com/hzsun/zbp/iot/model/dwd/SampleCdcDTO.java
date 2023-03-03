package com.hzsun.zbp.iot.model.dwd;

import com.hzsun.zbp.iot.utils.TimeUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.text.ParseException;

/**
  * REMARK   ods - dw 数据模型
  * @className   SampleCdcDTO
  * @date  2022/4/29 19:53
  * @author  cyf
  */
public class SampleCdcDTO {


    //ID
    private String id;

    private String personId;


    private Integer servings;

    // 比实际时间 早 8h   真实应该 +8
    private String startDate;

    private String endDate;

    private String createDate;

    private String modifyDate;

    //状态 0:提交;1:留样完成;2:取样完成
    //private String status;
    //
    //private String delFlag;



    //ID
    //private String personId;



    public SampleCdcDTO(ObjectNode jsonNodes) throws ParseException {

        JsonNode afterData  =  jsonNodes.get("after");
        String s = afterData.get("id").toString().replace("\"", "");
        this.id = s;

        String person_id = afterData.get("person_id").toString().replace("\"", "");
        this.personId = person_id;

        String servings1 = afterData.get("servings").toString().replace("\"", "");
        this.servings = Integer.valueOf(servings1);


        String start_date = afterData.get("start_date").toString().replace("\"", "");
        this.startDate = TimeUtils.formatDate(start_date);

        String end_date = afterData.get("end_date").toString().replace("\"", "");
        this.endDate =  ("null".equals(end_date)) ? " " : TimeUtils.formatDate(end_date);

        String create_date = afterData.get("create_date").toString().replace("\"", "");
        this.createDate = TimeUtils.formatDate(create_date);

        String modify_date = afterData.get("modify_date").toString().replace("\"", "");


        this.modifyDate =   TimeUtils.formatDate(modify_date);



        //JsonNode jsonNode = jsonNodes.get("after").get("person_id");
        //String replace = jsonNode.toString().replace("\"", "");
        //
        //this.personId = replace;


    }


    public String getId() {
        return id;
    }

    public String getStartDate() {
        return startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public String getCreateDate() {
        return createDate;
    }

    public String getModifyDate() {
        return modifyDate;
    }

    public String getPersonId() {
        return personId;
    }


    public Integer getServings() {
        return servings;
    }

    @Override
    public String toString() {
        return "{" +
                id +
                "," + personId +
                "," + servings +
                "," + startDate +
                "," + endDate +
                "," + createDate +
                "," + modifyDate +
                "}";
    }
}
