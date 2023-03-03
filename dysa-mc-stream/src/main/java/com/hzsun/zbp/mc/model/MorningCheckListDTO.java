package com.hzsun.zbp.mc.model;

import java.sql.Timestamp;


//  2 flink 接收dto 用于流 水印 和 map k 去重
public class MorningCheckListDTO {

    //ID
    public String sna;
    //ID
    public String id;
    //餐饮单位ID
    public Integer totalnum;
    //员工ID
    public Integer checknum;
    //设备编号
    public Integer errbno;
    // 检测类型
    public Double ckRate;
    // 检测类型
    public Double okRate;


    public Timestamp timestamp;




    public MorningCheckListDTO(String sna, String id, Integer totalnum, Integer checknum, Integer errbno, Double ckRate, Double okRate, Timestamp timestamp) {
        this.sna = sna;
        this.id = id;
        this.totalnum = totalnum;
        this.checknum = checknum;
        this.errbno = errbno;
        this.ckRate = ckRate;
        this.okRate = okRate;
        this.timestamp = timestamp;
    }


    //  tostring
    @Override
    public String toString() {

        //  "okRate":

        return "{" +
                "\"schId\":"+"\""+ sna  +"\""+","+
                "\"id\":"+"\"" + id  +"\""+","+
                "\"staffNum\":"  + totalnum +","+
                "\"checkNum\":" + checknum +","+
                "\"noPassNum\":"  + errbno +","+
                "\"execRate\":"  + ckRate +","+
                "\"passRate\":" + okRate +
                "}";
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getSna() {
        return sna;
    }

    public Integer getTotalnum() {
        return totalnum;
    }

    public Integer getChecknum() {
        return checknum;
    }

    public Integer getErrbno() {
        return errbno;
    }

    public Double getCkRate() {
        return ckRate;
    }

    public Double getOkRate() {
        return okRate;
    }

    public void setSna(String sna) {
        this.sna = sna;
    }

    public void setTotalnum(Integer totalnum) {
        this.totalnum = totalnum;
    }

    public void setChecknum(Integer checknum) {
        this.checknum = checknum;
    }

    public void setErrbno(Integer errbno) {
        this.errbno = errbno;
    }

    public void setCkRate(Double ckRate) {
        this.ckRate = ckRate;
    }

    public void setOkRate(Double okRate) {
        this.okRate = okRate;
    }
}
