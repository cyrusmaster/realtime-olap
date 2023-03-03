package com.hzsun.zbp.iot.model.ads;

import java.sql.Timestamp;

public class ListVO {

    private String schId;

    private String schName;

    private Integer sampleNum;

    private Integer getCnt;

    private Double standardRate;

    private Timestamp rowTime;


    public ListVO(String schId, String schName, Integer sampleNum, Integer getCnt, Double standardRate, Timestamp rowTime) {
        this.schId = schId;
        this.schName = schName;
        this.sampleNum = sampleNum;
        this.getCnt = getCnt;
        this.standardRate = standardRate;
        this.rowTime = rowTime;
    }



    @Override
    public String toString() {
        return "{" +
                "\"schId\":" +"\""+ schId  +"\"" +
                ",\"schName\":"  +"\""+ schName +"\"" +
                ",\"sampleNum\":" + sampleNum +
                ",\"getCnt\":" + getCnt +
                ",\"standardRate\":" + standardRate +
                "}";
    }



    public String timePrint() {
        return

                "{" +
                        "\"schId\":" + schId +
                        ",\"schName\":" + schName +
                        ",\"sampleNum\":" + sampleNum +
                        ",\"getCnt\":" + getCnt +
                        ",\"standardRate\":" + standardRate +
                        ",\"rowTime\":" + rowTime +
                        "}";
    }

    public String getSchId() {
        return schId;
    }

    public String getSchName() {
        return schName;
    }

    public Integer getSampleNum() {
        return sampleNum;
    }

    public Integer getGetCnt() {
        return getCnt;
    }

    public Double getStandardRate() {
        return standardRate;
    }

    public Timestamp getRowTime() {
        return rowTime;
    }
}
