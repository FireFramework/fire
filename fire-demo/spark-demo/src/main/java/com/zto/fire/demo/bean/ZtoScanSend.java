package com.zto.fire.demo.bean;


import com.alibaba.fastjson.annotation.JSONField;
import com.zto.fire.common.anno.FieldName;
import com.zto.fire.common.bean.HBaseBaseBean;
import com.zto.fire.common.util.DateFormatUtils;
import com.zto.fire.common.util.HBaseUtils;
import org.apache.commons.lang3.StringUtils;


/**
 * basescan:zto_scan_send  发包表
 * Created by ChengLong on 2017-11-28.
 */
public class ZtoScanSend extends HBaseBaseBean<ZtoScanSend> {
    /**
     * 运单编号
     */
    @FieldName("bill_code")
    private String bill_code;

    /**
     * 所属包编号
     */
    @FieldName("owner_bag_no")
    private String owner_bag_no;

    /**
     * 扫描类型
     */
    @FieldName("scan_type")
    private String scan_type;

    /**
     * 上一站或下一站
     */
    @FieldName("pre_or_next_station")
    private String pre_or_next_station;

    /**
     * 上一站或下一站网点ID
     */
    @FieldName("pre_or_nex_sta_id")
    private String pre_or_nex_sta_id;

    /**
     * 上一站或下一站省份ID
     */
    @FieldName("prep_province_id")
    private String prep_province_id;

    /**
     * 扫描人员工编号
     */
    @FieldName("scan_man_code")
    private String scan_man_code;

    /**
     * 扫描人员
     */
    @FieldName("scan_man")
    private String scan_man;

    /**
     * 扫描网点
     */
    @FieldName("scan_site")
    private String scan_site;

    /**
     * 扫描网点ID
     */
    @FieldName("scan_site_id")
    private String scan_site_id;

    /**
     * 扫描网点所属省份ID
     */
    @FieldName("scan_province_id")
    private String scan_province_id;

    /**
     * 扫描时间
     */
    @FieldName("scan_date")
    private String scan_date;

    /**
     * 录入时间
     */
    @FieldName("register_date")
    private String register_date;

    /**
     * 件数
     */
    @FieldName("piece")
    private String piece;

    /**
     * 重量
     */
    @FieldName("weight")
    private Double weight;

    /**
     * 物品类别
     */
    @FieldName("goods_type")
    private String goods_type;

    /**
     * 快件类型
     */
    @FieldName("fast_type")
    private String fast_type;

    /**
     * 班次
     */
    @FieldName("classes")
    private String classes;

    /**
     * PDA设备编号
     */
    @FieldName("pda_code")
    private String pda_code;

    /**
     * 数据来源(0:电子称 1:有线 2:无线 等)
     */
    @FieldName("data_from")
    private String data_from;

    /**
     * 车牌号/车签号
     */
    @FieldName("car_code")
    private String car_code;

    /**
     * 货代编号
     */
    @FieldName("agent_no")
    private String agent_no;

    /**
     * 货代名称
     */
    @FieldName("agent_name")
    private String agent_name;

    /**
     * 货代流水号
     */
    @FieldName("agent_serial")
    private String agent_serial;

    /**
     * 实际重量
     */
    @FieldName("fact_weight")
    private Double fact_weight;

    /**
     * 修改人员
     */
    @FieldName("modifiedusername")
    private String modifiedusername;

    /**
     * 修改人员编号
     */
    @FieldName("modifiedby")
    private String modifiedby;

    /**
     * 修改网点
     */
    @FieldName("modifiedsite")
    private String modifiedsite;

    /**
     * 修改时间
     */
    @FieldName("modifiedon")
    private String modifiedon;

    /**
     * 是否增补记录
     */
    @FieldName("is_subjoin")
    private String is_subjoin;

    /**
     * 是否删除
     */
    @FieldName("is_delete")
    private String is_delete;

    /**
     * 是否重复
     */
    @FieldName("is_repeat")
    private String is_repeat;

    /**
     * 时间戳
     */
    @FieldName("time_stamp")
    private String time_stamp = DateFormatUtils.formatCurrentDateTime();

    @FieldName(value = "op_type",disuse = true)
    private String op_type;

    @FieldName("input_date")
    private String input_date;

    public ZtoScanSend() {
        this.is_subjoin = "0";
        this.is_delete = "0";
        this.is_repeat = "0";
    }

    public ZtoScanSend(String bill_code, String scan_site_id, String scan_date, String is_delete) {
        this.bill_code = bill_code;
        this.scan_site_id = scan_site_id;
        this.scan_date = scan_date;
        this.is_delete = is_delete;
    }

    public String getBill_code() {
        return bill_code;
    }

    public void setBill_code(String bill_code) {
        this.bill_code = bill_code;
    }

    public String getOwner_bag_no() {
        return owner_bag_no;
    }

    public void setOwner_bag_no(String owner_bag_no) {
        this.owner_bag_no = owner_bag_no;
    }

    public String getScan_type() {
        return scan_type;
    }

    public void setScan_type(String scan_type) {
        this.scan_type = scan_type;
    }

    public String getPre_or_next_station() {
        return pre_or_next_station;
    }

    public void setPre_or_next_station(String pre_or_next_station) {
        this.pre_or_next_station = pre_or_next_station;
    }

    public String getPre_or_nex_sta_id() {
        return pre_or_nex_sta_id;
    }

    public void setPre_or_nex_sta_id(String pre_or_nex_sta_id) {
        this.pre_or_nex_sta_id = pre_or_nex_sta_id;
    }

    public String getPrep_province_id() {
        return prep_province_id;
    }

    public void setPrep_province_id(String prep_province_id) {
        this.prep_province_id = prep_province_id;
    }

    public String getScan_man_code() {
        return scan_man_code;
    }

    public void setScan_man_code(String scan_man_code) {
        this.scan_man_code = scan_man_code;
    }

    public String getScan_man() {
        return scan_man;
    }

    public void setScan_man(String scan_man) {
        this.scan_man = scan_man;
    }

    public String getScan_site() {
        return scan_site;
    }

    public void setScan_site(String scan_site) {
        this.scan_site = scan_site;
    }

    public String getScan_site_id() {
        return scan_site_id;
    }

    public void setScan_site_id(String scan_site_id) {
        this.scan_site_id = scan_site_id;
    }

    public String getScan_province_id() {
        return scan_province_id;
    }

    public void setScan_province_id(String scan_province_id) {
        this.scan_province_id = scan_province_id;
    }

    public String getScan_date() {
        return scan_date;
    }

    public void setScan_date(String scan_date) {
        this.scan_date = scan_date;
    }

    public String getRegister_date() {
        return register_date;
    }

    public void setRegister_date(String register_date) {
        this.register_date = register_date;
    }

    public String getPiece() {
        return piece;
    }

    public void setPiece(String piece) {
        this.piece = piece;
    }

    public Double getWeight() {
        return weight;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }

    public String getGoods_type() {
        return goods_type;
    }

    public void setGoods_type(String goods_type) {
        this.goods_type = goods_type;
    }

    public String getFast_type() {
        return fast_type;
    }

    public void setFast_type(String fast_type) {
        this.fast_type = fast_type;
    }

    @JSONField(name = "CLASS")
    public String getClasses() {
        return classes;
    }

    @JSONField(name = "CLASS")
    public void setClasses(String classes) {
        this.classes = classes;
    }

    public String getPda_code() {
        return pda_code;
    }

    public void setPda_code(String pda_code) {
        this.pda_code = pda_code;
    }

    public String getData_from() {
        return data_from;
    }

    public void setData_from(String data_from) {
        this.data_from = data_from;
    }

    public String getCar_code() {
        return car_code;
    }

    public void setCar_code(String car_code) {
        this.car_code = car_code;
    }

    public String getAgent_no() {
        return agent_no;
    }

    public void setAgent_no(String agent_no) {
        this.agent_no = agent_no;
    }

    public String getAgent_name() {
        return agent_name;
    }

    public void setAgent_name(String agent_name) {
        this.agent_name = agent_name;
    }

    public String getAgent_serial() {
        return agent_serial;
    }

    public void setAgent_serial(String agent_serial) {
        this.agent_serial = agent_serial;
    }

    public Double getFact_weight() {
        return fact_weight;
    }

    public void setFact_weight(Double fact_weight) {
        this.fact_weight = fact_weight;
    }

    public String getModifiedusername() {
        return modifiedusername;
    }

    public void setModifiedusername(String modifiedusername) {
        this.modifiedusername = modifiedusername;
    }

    public String getModifiedby() {
        return modifiedby;
    }

    public void setModifiedby(String modifiedby) {
        this.modifiedby = modifiedby;
    }

    public String getModifiedsite() {
        return modifiedsite;
    }

    public void setModifiedsite(String modifiedsite) {
        this.modifiedsite = modifiedsite;
    }

    public String getModifiedon() {
        return modifiedon;
    }

    public void setModifiedon(String modifiedon) {
        this.modifiedon = modifiedon;
    }

    public String getIs_subjoin() {
        return is_subjoin;
    }

    public void setIs_subjoin(String is_subjoin) {
        this.is_subjoin = is_subjoin;
    }

    public String getIs_delete() {
        return is_delete;
    }

    public void setIs_delete(String is_delete) {
        this.is_delete = is_delete;
    }

    public String getIs_repeat() {
        return is_repeat;
    }

    public void setIs_repeat(String is_repeat) {
        this.is_repeat = is_repeat;
    }

    public String getTime_stamp() {
        return time_stamp;
    }

    public void setTime_stamp(String time_stamp) {
        this.time_stamp = time_stamp;
    }

    public String getOp_type() {
        return op_type;
    }

    public void setOp_type(String op_type) {
        this.op_type = op_type;
    }

    public String getInput_date() {
        return input_date;
    }

    public void setInput_date(String input_date) {
        this.input_date = input_date;
    }

    @Override
    public ZtoScanSend buildRowKey() {
        this.rowKey = HBaseUtils.appendString(StringUtils.reverse(this.bill_code), "0", 16) + DateFormatUtils.oggDateSchemaFormat(this.scan_date, "yyyyMMddHHmmss") + this.scan_site_id;
        return this;
    }
}
