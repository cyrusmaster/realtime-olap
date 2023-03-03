package com.hzsun.zbp.ord.model.table;

import java.sql.Timestamp;

public class TopTableDTO {

    public Double executeRate;

    public Double qualifiedRate;

    public Double notStandardRate;

    public Timestamp rowTime;


    @Override
    public String toString() {
        return "{" +
                "executeRate=" + executeRate +
                ", qualifiedRate=" + qualifiedRate +
                ", notStandardRate=" + notStandardRate +
                ", rowTime=" + rowTime +
                '}';
    }
}
