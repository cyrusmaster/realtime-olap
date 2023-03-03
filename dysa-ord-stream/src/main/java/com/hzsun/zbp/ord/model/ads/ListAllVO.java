package com.hzsun.zbp.ord.model.ads;

public class ListAllVO {

    private String schId;

    private String schName;

    private Double executeRate;

    private Double qualifiedRate;

    private Double notStandardRate;


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

    public ListAllVO(String schId, String schName, Double executeRate, Double qualifiedRate, Double notStandardRate) {
        this.schId = schId;
        this.schName = schName;
        this.executeRate = executeRate;
        this.qualifiedRate = qualifiedRate;
        this.notStandardRate = notStandardRate;
    }
}
