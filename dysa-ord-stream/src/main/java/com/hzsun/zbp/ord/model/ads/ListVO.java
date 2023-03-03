package com.hzsun.zbp.ord.model.ads;

import java.sql.Timestamp;

public class ListVO {

    private String schId;

    private String schName;

    private Double executeRate;

    private Double qualifiedRate;

    private Double notStandardRate;

    private Timestamp rowTime;


    public Timestamp getRowTime() {
        return rowTime;
    }

    public String getSchId() {
        return schId;
    }

    public String getSchName() {
        return schName;
    }

    public Double getExecuteRate() {
        return executeRate;
    }

    public Double getQualifiedRate() {
        return qualifiedRate;
    }

    public Double getNotStandardRate() {
        return notStandardRate;
    }

    public ListVO(String schId, String schName, Double executeRate, Double qualifiedRate, Double notStandardRate, Timestamp rowTime) {
        this.schId = schId;
        this.schName = schName;
        this.executeRate = executeRate;
        this.qualifiedRate = qualifiedRate;
        this.notStandardRate = notStandardRate;
        this.rowTime = rowTime;
    }

    @Override
    public String toString() {
        return "{" +
                "\"schId\":" +"\""+ schId  +"\"" +
                ",\"schName\":"  +"\""+ schName +"\"" +
                ",\"executeRate\":" + executeRate +
                ",\"qualifiedRate\":" + qualifiedRate +
                ",\"notStandardRate\":" + notStandardRate +
                "}";
    }
}
