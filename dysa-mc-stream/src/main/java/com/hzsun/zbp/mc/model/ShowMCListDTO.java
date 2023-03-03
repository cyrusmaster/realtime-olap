package com.hzsun.zbp.mc.model;

public class ShowMCListDTO {


    //ID
    private String schId;
    //ID
    public String id;
    //餐饮单位ID
    private Integer staffNum;
    //员工ID
    private Integer checkNum;
    //设备编号
    private Integer noPassNum;
    // 检测类型
    private Double execRate;
    // 检测类型
    private Double passRate;


    public ShowMCListDTO(){}

    public ShowMCListDTO(ShowMCListDTO showMCListDTO) {
        this.schId = showMCListDTO.schId;
        this.id = showMCListDTO.id;
        this.staffNum = showMCListDTO.staffNum;
        this.checkNum = showMCListDTO.checkNum;
        this.noPassNum = showMCListDTO.noPassNum;
        this.execRate = showMCListDTO.execRate;
        this.passRate = showMCListDTO.passRate;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setSna(String sna) {
        this.schId = sna;
    }

    public void setTotalnum(Integer totalnum) {
        this.staffNum = totalnum;
    }

    public void setChecknum(Integer checknum) {
        this.checkNum = checknum;
    }

    public void setErrbno(Integer errbno) {
        this.noPassNum = errbno;
    }

    public void setCkRate(Double ckRate) {
        this.execRate = ckRate;
    }

    public void setOkRate(Double okRate) {
        this.passRate = okRate;
    }

    public String getSna() {
        return schId;
    }

    public Integer getTotalnum() {
        return staffNum;
    }

    public Integer getChecknum() {
        return checkNum;
    }

    public Integer getErrbno() {
        return noPassNum;
    }

    public Double getCkRate() {
        return execRate;
    }

    public Double getOkRate() {
        return passRate;
    }

    @Override
    public String toString() {
        return
                "{" +
                "\"schId\":"+ schId +","+
                "\"staffNum\":"  + staffNum +","+
                "\"checkNum\":" + checkNum +","+
                "\"noPassNum\":"  + noPassNum +","+
                "\"execRate\":"  + execRate +","+
                "\"passRate\":" + passRate +
                "}";
    }


}
