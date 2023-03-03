package com.hzsun.zbp.ord.model.ads;



public class TopVO {


    private Double executeRate;

    private Double qualifiedRate;

    private Double notStandardRate;


    public TopVO(Double executeRate, Double qualifiedRate, Double notStandardRate) {
        this.executeRate = executeRate;
        this.qualifiedRate = qualifiedRate;
        this.notStandardRate = notStandardRate;
    }


    @Override
    public String toString() {
        return "{" +
                "\"executeRate\":" + executeRate +
                ",\"qualifiedRate\":" + qualifiedRate +
                ",\"notStandardRate\":" + notStandardRate +
                "}";
    }
}
