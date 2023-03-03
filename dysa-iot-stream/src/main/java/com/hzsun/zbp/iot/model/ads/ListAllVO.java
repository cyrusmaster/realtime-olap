package com.hzsun.zbp.iot.model.ads;

import java.sql.Timestamp;

public class ListAllVO {

    private String schId;

    private String schName;

    private Integer sampleNum;

    private Integer getCnt;

    private Double standardRate;

    //private Timestamp rowTime;

    public ListAllVO(String schId, String schName, Integer sampleNum, Integer getCnt, Double standardRate) {
        this.schId = schId;
        this.schName = schName;
        this.sampleNum = sampleNum;
        this.getCnt = getCnt;
        this.standardRate = standardRate;
        //this.rowTime = rowTime;
    }

    public void setSchId(String schId) {
        this.schId = schId;
    }

    public void setSchName(String schName) {
        this.schName = schName;
    }

    public void setSampleNum(Integer sampleNum) {
        this.sampleNum = sampleNum;
    }

    public void setGetCnt(Integer getCnt) {
        this.getCnt = getCnt;
    }

    public void setStandardRate(Double standardRate) {
        this.standardRate = standardRate;
    }

    //public void setRowTime(Timestamp rowTime) {
    //    this.rowTime = rowTime;
    //}


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
}
