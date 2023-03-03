package com.hzsun.zbp.iot.model.table;

import java.sql.Timestamp;

public class ListTO {

    public String schId;

    public String schName;

    public Integer sampleNum;

    public Integer getCnt;

    public Double standardRate;

    public Timestamp rowTime;


    @Override
    public String toString() {
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





}
