package com.hzsun.zbp.ord.model.table;


import java.sql.Timestamp;

public class ListTableDTO {


    public String schId;

    public String schName;

    public Double executeRate;

    public Double qualifiedRate;

    public Double notStandardRate;

    public Timestamp rowTime;


    @Override
    public String toString() {
        return "ListTableDTO{" +
                "schId='" + schId + '\'' +
                ", schName='" + schName + '\'' +
                ", executeRate=" + executeRate +
                ", qualifiedRate=" + qualifiedRate +
                ", notStandardRate=" + notStandardRate +
                ", rowTime=" + rowTime +
                '}';
    }
}
