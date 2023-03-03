package com.hzsun.zbp.mc.model;

import java.sql.Timestamp;

// 除了字段什么都不要有 否则报错
public class MorningCheckTopTDTO {

    //ID
    public Integer num;
    //餐饮单位ID
    public Integer ckNum;
    //员工ID
    public Double mcRate;
    //设备编号
    public Double okRate;

    public Timestamp rowTime;


    @Override
    public String toString() {
        return "{" +
                num +
                "," + ckNum +
                "," + mcRate +
                "," + okRate +
                "," + rowTime +
                "}";
    }
}
