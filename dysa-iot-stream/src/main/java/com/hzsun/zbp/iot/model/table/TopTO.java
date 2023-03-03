package com.hzsun.zbp.iot.model.table;

import java.sql.Timestamp;

public class TopTO {

    public Integer sampleNum;

    public Integer getCnt;

    public Double standardRate;

    public Timestamp rowTime;


    @Override
    public String toString() {
        return "{" +
                sampleNum +
                "," + getCnt +
                "," + standardRate +
                "," + rowTime +
                "}";
    }
}
