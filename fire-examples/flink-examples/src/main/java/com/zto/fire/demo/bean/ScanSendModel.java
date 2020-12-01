package com.zto.fire.demo.bean;

import com.zto.fire.common.anno.FieldName;

import java.io.Serializable;

/**
 * @author bingzhikun
 */
public class ScanSendModel implements Serializable {

    @FieldName("bill_code")
    private String billCode;
    @FieldName("owner_bag_no")
    private String ownerBagNo;
    @FieldName("scan_type")
    private String scanType;
    @FieldName("pre_or_next_station")
    private String preOrNextStation;
    @FieldName("pre_or_nex_sta_id")
    private Long preOrNexStaId;
    @FieldName("prep_province_id")
    private Long prepProvinceId;
    @FieldName("scan_man_code")
    private String scanManCode;
    @FieldName("scan_man")
    private String scanMan;
    @FieldName("scan_site")
    private String scanSite;
    @FieldName("scan_site_id")
    private Long scanSiteId;
    @FieldName("scan_province_id")
    private Long scanProvinceId;
    @FieldName("scan_date")
    private String scanDate;
    @FieldName("register_date")
    private String registerDate;
    @FieldName("dispatch_or_send_man")
    private String dispatchOrSendMan;
    @FieldName("customer_name")
    private String customerName;
    @FieldName("piece")
    private Long piece;
    @FieldName("weight")
    private Double weight;
    @FieldName("goods_type")
    private Integer goodsType;
    @FieldName("fast_type")
    private Integer fastType;
    @FieldName("class")
    private String clazz;
    @FieldName("dispatch_id")
    private Long dispatchId;
    @FieldName("dispatch_site")
    private String dispatchSite;
    @FieldName("lwh")
    private String lwh;
    @FieldName("volume_weight")
    private Double volumeWeight;
    @FieldName("bl_return_bill_id")
    private Long blReturnBillId;
    @FieldName("bl_untread_piece_id")
    private Long blUnTreadPieceId;
    @FieldName("pda_code")
    private String pdaCode;
    @FieldName("data_from")
    private String dataFrom;
    @FieldName("car_code")
    private String carCode;
    @FieldName("remark")
    private String remark;
    @FieldName("transfere_bill_code")
    private String transferBillCode;
    @FieldName("agent_no")
    private String agentNo;
    @FieldName("agent_name")
    private String agentName;
    @FieldName("fact_weight")
    private Double factWeight;
    @FieldName("agent_serial")
    private String agentSerial;
    @FieldName("disorsen_man_code")
    private String disOrSenManCode;
    @FieldName("modifiedusername")
    private String modifiedUserName;
    @FieldName("modifiedby")
    private String modifiedBy;
    @FieldName("modifiedsite")
    private String modifiedSite;
    @FieldName("modifiedon")
    private String modifiedOn;
    @FieldName("input_date")
    private String inputDate;

    public String getBillCode() {
        return billCode;
    }

    public void setBillCode(String billCode) {
        this.billCode = billCode;
    }

    public String getOwnerBagNo() {
        return ownerBagNo;
    }

    public void setOwnerBagNo(String ownerBagNo) {
        this.ownerBagNo = ownerBagNo;
    }

    public String getScanType() {
        return scanType;
    }

    public void setScanType(String scanType) {
        this.scanType = scanType;
    }

    public String getPreOrNextStation() {
        return preOrNextStation;
    }

    public void setPreOrNextStation(String preOrNextStation) {
        this.preOrNextStation = preOrNextStation;
    }

    public Long getPreOrNexStaId() {
        return preOrNexStaId;
    }

    public void setPreOrNexStaId(Long preOrNexStaId) {
        this.preOrNexStaId = preOrNexStaId;
    }

    public Long getPrepProvinceId() {
        return prepProvinceId;
    }

    public void setPrepProvinceId(Long prepProvinceId) {
        this.prepProvinceId = prepProvinceId;
    }

    public String getScanManCode() {
        return scanManCode;
    }

    public void setScanManCode(String scanManCode) {
        this.scanManCode = scanManCode;
    }

    public String getScanMan() {
        return scanMan;
    }

    public void setScanMan(String scanMan) {
        this.scanMan = scanMan;
    }

    public String getScanSite() {
        return scanSite;
    }

    public void setScanSite(String scanSite) {
        this.scanSite = scanSite;
    }

    public Long getScanSiteId() {
        return scanSiteId;
    }

    public void setScanSiteId(Long scanSiteId) {
        this.scanSiteId = scanSiteId;
    }

    public Long getScanProvinceId() {
        return scanProvinceId;
    }

    public void setScanProvinceId(Long scanProvinceId) {
        this.scanProvinceId = scanProvinceId;
    }

    public String getScanDate() {
        return scanDate;
    }

    public void setScanDate(String scanDate) {
        this.scanDate = scanDate;
    }

    public String getRegisterDate() {
        return registerDate;
    }

    public void setRegisterDate(String registerDate) {
        this.registerDate = registerDate;
    }

    public String getDispatchOrSendMan() {
        return dispatchOrSendMan;
    }

    public void setDispatchOrSendMan(String dispatchOrSendMan) {
        this.dispatchOrSendMan = dispatchOrSendMan;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public Long getPiece() {
        return piece;
    }

    public void setPiece(Long piece) {
        this.piece = piece;
    }

    public Double getWeight() {
        return weight;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }

    public Integer getGoodsType() {
        return goodsType;
    }

    public void setGoodsType(Integer goodsType) {
        this.goodsType = goodsType;
    }

    public Integer getFastType() {
        return fastType;
    }

    public void setFastType(Integer fastType) {
        this.fastType = fastType;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    public Long getDispatchId() {
        return dispatchId;
    }

    public void setDispatchId(Long dispatchId) {
        this.dispatchId = dispatchId;
    }

    public String getDispatchSite() {
        return dispatchSite;
    }

    public void setDispatchSite(String dispatchSite) {
        this.dispatchSite = dispatchSite;
    }

    public String getLwh() {
        return lwh;
    }

    public void setLwh(String lwh) {
        this.lwh = lwh;
    }

    public Double getVolumeWeight() {
        return volumeWeight;
    }

    public void setVolumeWeight(Double volumeWeight) {
        this.volumeWeight = volumeWeight;
    }

    public Long getBlReturnBillId() {
        return blReturnBillId;
    }

    public void setBlReturnBillId(Long blReturnBillId) {
        this.blReturnBillId = blReturnBillId;
    }

    public Long getBlUnTreadPieceId() {
        return blUnTreadPieceId;
    }

    public void setBlUnTreadPieceId(Long blUnTreadPieceId) {
        this.blUnTreadPieceId = blUnTreadPieceId;
    }

    public String getPdaCode() {
        return pdaCode;
    }

    public void setPdaCode(String pdaCode) {
        this.pdaCode = pdaCode;
    }

    public String getAgentNo() {
        return agentNo;
    }

    public void setAgentNo(String agentNo) {
        this.agentNo = agentNo;
    }

    public String getAgentName() {
        return agentName;
    }

    public void setAgentName(String agentName) {
        this.agentName = agentName;
    }

    public String getAgentSerial() {
        return agentSerial;
    }

    public void setAgentSerial(String agentSerial) {
        this.agentSerial = agentSerial;
    }

    public Double getFactWeight() {
        return factWeight;
    }

    public void setFactWeight(Double factWeight) {
        this.factWeight = factWeight;
    }

    public String getDisOrSenManCode() {
        return disOrSenManCode;
    }

    public void setDisOrSenManCode(String disOrSenManCode) {
        this.disOrSenManCode = disOrSenManCode;
    }

    public String getModifiedUserName() {
        return modifiedUserName;
    }

    public void setModifiedUserName(String modifiedUserName) {
        this.modifiedUserName = modifiedUserName;
    }

    public String getModifiedBy() {
        return modifiedBy;
    }

    public void setModifiedBy(String modifiedBy) {
        this.modifiedBy = modifiedBy;
    }

    public String getModifiedSite() {
        return modifiedSite;
    }

    public void setModifiedSite(String modifiedSite) {
        this.modifiedSite = modifiedSite;
    }

    public String getModifiedOn() {
        return modifiedOn;
    }

    public void setModifiedOn(String modifiedOn) {
        this.modifiedOn = modifiedOn;
    }

    public String getInputDate() {
        return inputDate;
    }

    public void setInputDate(String inputDate) {
        this.inputDate = inputDate;
    }

    public String getDataFrom() {
        return dataFrom;
    }

    public void setDataFrom(String dataFrom) {
        this.dataFrom = dataFrom;
    }

    public String getCarCode() {
        return carCode;
    }

    public void setCarCode(String carCode) {
        this.carCode = carCode;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getTransferBillCode() {
        return transferBillCode;
    }

    public void setTransferBillCode(String transferBillCode) {
        this.transferBillCode = transferBillCode;
    }
}
