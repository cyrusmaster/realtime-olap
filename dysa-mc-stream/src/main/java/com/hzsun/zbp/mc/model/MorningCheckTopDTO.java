package com.hzsun.zbp.mc.model;

import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.Timestamp;


public class MorningCheckTopDTO {


    //ID
    public Integer num;

    //餐饮单位ID
    public Integer ckNum;

    //执行
    public Double mcRate;

    //通过率
    public Double okRate;

    public Timestamp rowTime;


    public MorningCheckTopDTO(Integer num, Integer ckNum, Double mcRate, Double okRate, Timestamp rowTime) {



            this.num = num;
            this.ckNum = ckNum;
            this.mcRate = mcRate;
            this.okRate = okRate;
            this.rowTime = rowTime;


    }


    public MorningCheckTopDTO() {
        this.num = 0;
        this.ckNum = 0;
        this.mcRate = 0.0;
        this.okRate = 0.0;
        String timeStr = "2010-06-23 13:18:33.112233";
        this.rowTime = Timestamp.valueOf(timeStr) ;
    }


    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public Integer getCkNum() {
        return ckNum;
    }

    public void setCkNum(Integer ckNum) {
        this.ckNum = ckNum;
    }

    public Double getMcRate() {
        return mcRate;
    }

    public void setMcRate(Double mcRate) {
        this.mcRate = mcRate;
    }

    public Double getOkRate() {
        return okRate;
    }

    public void setOkRate(Double okRate) {
        this.okRate = okRate;
    }

    public Timestamp getRowTime() {
        return rowTime;
    }

    public void setRowTime(Timestamp rowTime) {
        this.rowTime = rowTime;
    }

    @Override
    public String toString() {
        return "{" +
                "\"staffNum\":"  + num +
                ",\"checkNum\":"  + ckNum +
                ",\"execRate\":" + mcRate +
                ",\"passRate\":" + okRate +
                "}";
    }
}
