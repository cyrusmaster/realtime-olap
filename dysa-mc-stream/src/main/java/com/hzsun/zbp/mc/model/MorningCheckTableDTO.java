package com.hzsun.zbp.mc.model;


import java.sql.Timestamp;

// 用于 flink table 传入  除了字段什么都不要有 否则报错
public class MorningCheckTableDTO {

    //ID
    public String sna;
    //ID
    public String id;
    //餐饮单位ID
    public Integer totalnum;
    //员工ID
    public Integer checknum;
    //设备编号
    public Integer errbno;
    // 检测类型
    public Double ckRate;
    // 检测类型
    public Double okRate;

    // 检测类型
    public Timestamp row_time;

}
