package com.hzsun.zbp.iot.model.ads;

public class TopVO {

    private Integer sampleNum;

    private Integer getCnt;

    private Double standardRate;


    public TopVO(Integer sampleNum, Integer getCnt, Double standardRate) {
        this.sampleNum = sampleNum;
        this.getCnt = getCnt;
        this.standardRate = standardRate;
    }


    @Override
    public String toString() {
        return "{" +
                "\"sampleNum\":" + sampleNum +
                ",\"getCnt\":" + getCnt +
                ",\"standardRate\":" + standardRate +
                "}";
    }
}
