package com.zto.fire.demo.bean;

import com.alibaba.fastjson.JSON;
import com.zto.fire.common.anno.FieldName;
import com.zto.fire.common.bean.HBaseBaseBean;

import java.math.BigDecimal;

/**
 * 大数据消息队列使用实体
 *
 * @author ChengLong
 * @date 2017年03月10日10:24:02
 */
public class Senda extends HBaseBaseBean<Senda> {

    @Override
    public Senda buildRowKey() {
        this.rowKey = this.bill_code + "";
        return this;
    }

    /**
     * 单号<br>
     * 表字段 : zto_site_senda_bills.bill_code
     */
    @FieldName(value = "bill_code", comment = "单号")
    private String bill_code;
    /**
     * 寄件网点id<br>
     * 表字段 : zto_site_senda_bills.scan_site_id
     */
    private Long scan_site_id;
    /**
     * 包号<br>
     * 表字段 : zto_site_senda_bills.owner_bag_no
     */
    @FieldName("owner_bag_no")
    private String owner_bag_no;
    /**
     * 寄件时间<br>
     * 表字段 : zto_site_senda_bills.scan_date
     */
    @FieldName("scan_date")
    private String scan_date;
    /**
     * 揽件人id<br>
     * 表字段 : zto_site_senda_bills.rec_man_id
     */
    @FieldName("rec_man_id")
    private Long rec_man_id;
    /**
     * 结算重量<br>
     * 表字段 : zto_site_senda_bills.account_weight
     */
    @FieldName("account_weight")
    private BigDecimal account_weight;
    /**
     * 运输方式 1是汽运 2是航空<br>
     * 表字段 : zto_site_senda_bills.class_type
     */
    @FieldName("class_type")
    private Integer class_type;
    /**
     * 退件状态0否1是<br>
     * 表字段 : zto_site_senda_bills.return_status
     */
    @FieldName("return_status")
    private Integer return_status;
    /**
     * 客户系统费用<br>
     * 表字段 : zto_site_senda_bills.customer_stm_fee
     */
    @FieldName("customer_stm_fee")
    private BigDecimal customer_stm_fee;
    /**
     * 承包区面单费<br>
     * 表字段 : zto_site_senda_bills.area_bill_fee
     */
    @FieldName("area_bill_fee")
    private BigDecimal area_bill_fee;
    /**
     * 承包区中转费<br>
     * 表字段 : zto_site_senda_bills.area_center_fee
     */
    @FieldName("area_center_fee")
    private BigDecimal area_center_fee;
    /**
     * 承包区操作费<br>
     * 表字段 : zto_site_senda_bills.area_oper_fee
     */
    @FieldName("area_oper_fee")
    private BigDecimal area_oper_fee;
    /**
     * 普通散客费<br>
     * 表字段 : zto_site_senda_bills.common_fee
     */
    @FieldName("common_fee")
    private BigDecimal common_fee;
    /**
     * 均重费<br>
     * 表字段 : zto_site_senda_bills.avg_weight_fee
     */
    @FieldName("avg_weight_fee")
    private BigDecimal avg_weight_fee;
    /**
     * 均重加收费<br>
     * 表字段 : zto_site_senda_bills.add_avg_weight_fee
     */
    @FieldName("add_avg_weight_fee")
    private BigDecimal add_avg_weight_fee;

    /**
     * 面单客户id<br>
     * 表字段 : zto_site_senda_bills.customer_id
     */
    @FieldName("customer_id")
    private Long customer_id;
    /**
     * 签收时间<br>
     * 表字段 : zto_site_senda_bills.sign_date
     */
    @FieldName("sign_date")
    private String sign_date;
    /**
     * 签收网点id<br>
     * 表字段 : zto_site_senda_bills.sign_site_id
     */
    @FieldName("sign_site_id")
    private Long sign_site_id;
    /**
     * 收发重量<br>
     * 表字段 : zto_site_senda_bills.rec_weight
     */
    @FieldName("rec_weight")
    private BigDecimal rec_weight;
    /**
     * 中心重量差值，公式为：实际的中心重量减去收件重量<br>
     * 表字段 : zto_site_senda_bills.center_weight
     */
    @FieldName("center_weight")
    private BigDecimal center_weight;

    /**
     * 目的地ID<br>
     * 表字段 : zto_site_senda_bills.dispatch_id
     */
    @FieldName("dispatch_id")
    private Long dispatch_id;
    /**
     * 派件时间<br>
     * 表字段 : zto_site_senda_bills.disp_date
     */
    @FieldName("disp_date")
    private String disp_date;
    /**
     * 派件网点id<br>
     * 表字段 : zto_site_senda_bills.disp_site_id
     */
    @FieldName("disp_site_id")
    private Long disp_site_id;
    /**
     * 问题件时间<br>
     * 表字段 : zto_site_senda_bills.other_date
     */
    @FieldName("other_date")
    private String other_date;
    /**
     * 中心发件时间<br>
     * 表字段 : zto_site_senda_bills.center_send_date
     */
    @FieldName("center_send_date")
    private String center_send_date;
    /**
     * 中心扫描网点id<br>
     * 表字段 : zto_site_senda_bills.center_site_id
     */
    @FieldName("center_site_id")
    private Long center_site_id;

    /**
     * 业务员提成<br>
     * 表字段 : zto_site_senda_bills.man_fee
     */
    @FieldName("man_fee")
    private BigDecimal man_fee;
    /**
     * 有偿派费<br>
     * 表字段 : zto_site_senda_bills.disp_fee
     */
    @FieldName("disp_fee")
    private BigDecimal disp_fee;
    /**
     * 续重派费<br>
     * 表字段 : zto_site_senda_bills.add_disp_fee
     */
    @FieldName("add_disp_fee")
    private BigDecimal add_disp_fee;
    /**
     * 寄件单类型0散客1客户2承包区<br>
     * 表字段 : zto_site_senda_bills.type
     */
    @FieldName("type")
    private Integer type;
    /**
     * 0未计算1待计算2已计算3待核销4已核销<br>
     * 表字段 : zto_site_senda_bills.bill_status
     */
    @FieldName("bill_status")
    private Integer bill_status;
    /**
     * 面单类型0线上1普通<br>
     * 表字段 : zto_site_senda_bills.bill_type
     */
    @FieldName("bill_type")
    private Integer bill_type;
    /**
     * 是否计算均重费0否1是<br>
     * 表字段 : zto_site_senda_bills.avg_status
     */
    @FieldName("avg_status")
    private Integer avg_status;

    /**
     * 揽件人归属承包区id
     * 表字段 : zto_site_senda_bills.area_id
     */
    @FieldName("area_id")
    private Long area_id;

    /**
     * 面单发放客户id
     * 表字段 : zto_site_senda_bills.user_prov_customer_id
     */
    @FieldName("user_prov_customer_id")
    private Long user_prov_customer_id;

    /**
     * 结算id(结算周期表)
     * 表字段 : zto_site_senda_bills.account_way_id
     */
    @FieldName("account_way_id")
    private Long accountWayId;

    /**
     * 锁定费用0未锁定1锁定
     * 表字段 : zto_site_senda_bills.lock_status
     */
    @FieldName("lock_status")
    private Integer lock_status;

    /**
     * 中心收中转费
     * 字段 : zto_site_senda_bills.center_rec_fee
     */
    @FieldName("center_rec_fee")
    private BigDecimal center_rec_fee;

    /**
     * 中心收中费次数
     * 字段 : zto_site_senda_bills.center_rec_count
     */
    @FieldName("center_rec_count")
    private Integer center_rec_count;

    /**
     * 目的地
     * 字段 : zto_site_senda_bills.dispatch
     */
    @FieldName("dispatch")
    private String dispatch;

    /**
     * 派件重量
     * 字段 : zto_site_senda_bills.disp_weight
     */
    @FieldName("disp_weight")
    private BigDecimal disp_weight;

    /**
     * 中心扫描员code
     * 字段 : zto_site_senda_bills.center_scan_man_code
     */
    @FieldName("center_scan_man_code")
    private String center_scan_man_code;

    /**
     * 面单成本
     * 字段 : zto_site_senda_bills.surface_fee
     */
    @FieldName("surface_fee")
    private BigDecimal surface_fee;

    /**
     * 毛利
     * 字段 : zto_site_senda_bills.gross_fee
     */
    @FieldName("gross_fee")
    private BigDecimal gross_fee;

    /**
     * 到付款
     * 字段 : zto_site_senda_bills.goods_payment
     */
    @FieldName("goods_payment")
    private BigDecimal goods_payment;

    /**
     * 到付手续费
     * 字段 : zto_site_senda_bills.di_fee
     */
    @FieldName("di_fee")
    private BigDecimal di_fee;

    /**
     * 代收手续费
     * 字段 : zto_site_senda_bills.cod_fee
     */
    @FieldName("cod_fee")
    private BigDecimal cod_fee;

    /**
     * 代收货款
     * 字段 : zto_site_senda_bills.cod_payment
     */
    @FieldName("cod_payment")
    private BigDecimal cod_payment;

    /**
     * 数据来源1收2发3到
     * 字段 : zto_site_senda_bills.date_source
     */
    @FieldName("date_source")
    private Integer date_source;

    /**
     * 修改时间
     * 字段 : zto_site_senda_bills.gmt_modifie
     */
    @FieldName("gmt_modifie")
    private String gmt_modifie;

    /**
     * 修改人
     * 字段 : zto_site_senda_bills.editor_name
     */
    @FieldName("editor_name")
    private String editor_name;

    /**
     * 入库时间
     * 字段 : zto_site_senda_bills.gmt_create
     */
    @FieldName("gmt_create")
    private String gmt_create;

    /**
     * 收款人
     * 字段 : zto_site_senda_bills.proceeds_man
     */
    @FieldName("proceeds_man")
    private String proceeds_man;

    /**
     * 收款时间
     * 字段 : zto_site_senda_bills.proceeds_time
     */
    @FieldName("proceeds_time")
    private String proceeds_time;

    /**
     * 核销人
     * 字段 : zto_site_senda_bills.hexiao_man
     */
    @FieldName("hexiao_man")
    private String hexiao_man;

    /**
     * 核销时间
     * 字段 : zto_site_senda_bills.hexiao_time
     */
    @FieldName("hexiao_time")
    private String hexiao_time;

    /**
     * 结算用户
     * 字段 : zto_site_senda_bills.account_user_id
     */
    @FieldName("account_user_id")
    private Long account_user_id;

    /**
     * 更新时间
     * 字段 : zto_site_senda_bills.gmt_modified
     */
    @FieldName("gmt_modified")
    private String gmt_modified;

    /**
     * 客户面单费
     */
    @FieldName("customer_bill_fee")
    private BigDecimal customer_bill_fee;

    /**
     * 客户附加费
     */
    @FieldName("customer_add_fee")
    private BigDecimal customer_add_fee;

    /**
     * 面单发放客户名称（如果没有客户，那么只存一个名称）
     */
    @FieldName("user_prov_customer_name")
    private String user_prov_customer_name;

    @FieldName("city_id")
    private Long city_id;

    /**
     * 锁定或解锁人
     */
    @FieldName("lock_man")
    private String lock_man;

    /**
     * 锁定或解锁时间
     */
    @FieldName("lock_time")
    private String lock_time;

    /**
     * 目的地来源0未知1订单2建包发3中心发
     */
    @FieldName("dispatch_type")
    private Integer dispatch_type;

    /**
     * 订单来源0散单1线上2线下
     */
    @FieldName("order_type")
    private Integer order_type;

    /**
     * 二级中转费
     */
    @FieldName("two_level_fee")
    private BigDecimal two_level_fee;

    /**
     * 面单补贴
     */
    @FieldName("bill_subsidy")
    private BigDecimal bill_subsidy;

    /**
     * 分区
     */
    @FieldName("ds")
    private String ds;

    public String getDs() {
        return ds;
    }

    public void setDs(String ds) {
        this.ds = ds;
    }

    public String getBill_code() {
        return bill_code;
    }

    public void setBill_code(String bill_code) {
        this.bill_code = bill_code;
    }

    public Long getScan_site_id() {
        return scan_site_id;
    }

    public void setScan_site_id(Long scan_site_id) {
        this.scan_site_id = scan_site_id;
    }

    public String getOwner_bag_no() {
        return owner_bag_no;
    }

    public void setOwner_bag_no(String owner_bag_no) {
        this.owner_bag_no = owner_bag_no;
    }

    public String getScan_date() {
        return scan_date;
    }

    public void setScan_date(String scan_date) {
        this.scan_date = scan_date;
    }

    public Long getRec_man_id() {
        return rec_man_id;
    }

    public void setRec_man_id(Long rec_man_id) {
        this.rec_man_id = rec_man_id;
    }

    public BigDecimal getAccount_weight() {
        return account_weight;
    }

    public void setAccount_weight(BigDecimal account_weight) {
        this.account_weight = account_weight;
    }

    public Integer getClass_type() {
        return class_type;
    }

    public void setClass_type(Integer class_type) {
        this.class_type = class_type;
    }

    public Integer getReturn_status() {
        return return_status;
    }

    public void setReturn_status(Integer return_status) {
        this.return_status = return_status;
    }

    public BigDecimal getCustomer_stm_fee() {
        return customer_stm_fee;
    }

    public void setCustomer_stm_fee(BigDecimal customer_stm_fee) {
        this.customer_stm_fee = customer_stm_fee;
    }

    public BigDecimal getArea_bill_fee() {
        return area_bill_fee;
    }

    public void setArea_bill_fee(BigDecimal area_bill_fee) {
        this.area_bill_fee = area_bill_fee;
    }

    public BigDecimal getArea_center_fee() {
        return area_center_fee;
    }

    public void setArea_center_fee(BigDecimal area_center_fee) {
        this.area_center_fee = area_center_fee;
    }

    public BigDecimal getArea_oper_fee() {
        return area_oper_fee;
    }

    public void setArea_oper_fee(BigDecimal area_oper_fee) {
        this.area_oper_fee = area_oper_fee;
    }

    public BigDecimal getCommon_fee() {
        return common_fee;
    }

    public void setCommon_fee(BigDecimal common_fee) {
        this.common_fee = common_fee;
    }

    public BigDecimal getAvg_weight_fee() {
        return avg_weight_fee;
    }

    public void setAvg_weight_fee(BigDecimal avg_weight_fee) {
        this.avg_weight_fee = avg_weight_fee;
    }

    public BigDecimal getAdd_avg_weight_fee() {
        return add_avg_weight_fee;
    }

    public void setAdd_avg_weight_fee(BigDecimal add_avg_weight_fee) {
        this.add_avg_weight_fee = add_avg_weight_fee;
    }

    public Long getCustomer_id() {
        return customer_id;
    }

    public void setCustomer_id(Long customer_id) {
        this.customer_id = customer_id;
    }

    public String getSign_date() {
        return sign_date;
    }

    public void setSign_date(String sign_date) {
        this.sign_date = sign_date;
    }

    public Long getSign_site_id() {
        return sign_site_id;
    }

    public void setSign_site_id(Long sign_site_id) {
        this.sign_site_id = sign_site_id;
    }

    public BigDecimal getRec_weight() {
        return rec_weight;
    }

    public void setRec_weight(BigDecimal rec_weight) {
        this.rec_weight = rec_weight;
    }

    public BigDecimal getCenter_weight() {
        return center_weight;
    }

    public void setCenter_weight(BigDecimal center_weight) {
        this.center_weight = center_weight;
    }

    public Long getDispatch_id() {
        return dispatch_id;
    }

    public void setDispatch_id(Long dispatch_id) {
        this.dispatch_id = dispatch_id;
    }

    public String getDisp_date() {
        return disp_date;
    }

    public void setDisp_date(String disp_date) {
        this.disp_date = disp_date;
    }

    public Long getDisp_site_id() {
        return disp_site_id;
    }

    public void setDisp_site_id(Long disp_site_id) {
        this.disp_site_id = disp_site_id;
    }

    public String getOther_date() {
        return other_date;
    }

    public void setOther_date(String other_date) {
        this.other_date = other_date;
    }

    public String getCenter_send_date() {
        return center_send_date;
    }

    public void setCenter_send_date(String center_send_date) {
        this.center_send_date = center_send_date;
    }

    public Long getCenter_site_id() {
        return center_site_id;
    }

    public void setCenter_site_id(Long center_site_id) {
        this.center_site_id = center_site_id;
    }

    public BigDecimal getMan_fee() {
        return man_fee;
    }

    public void setMan_fee(BigDecimal man_fee) {
        this.man_fee = man_fee;
    }

    public BigDecimal getDisp_fee() {
        return disp_fee;
    }

    public void setDisp_fee(BigDecimal disp_fee) {
        this.disp_fee = disp_fee;
    }

    public BigDecimal getAdd_disp_fee() {
        return add_disp_fee;
    }

    public void setAdd_disp_fee(BigDecimal add_disp_fee) {
        this.add_disp_fee = add_disp_fee;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Integer getBill_status() {
        return bill_status;
    }

    public void setBill_status(Integer bill_status) {
        this.bill_status = bill_status;
    }

    public Integer getBill_type() {
        return bill_type;
    }

    public void setBill_type(Integer bill_type) {
        this.bill_type = bill_type;
    }

    public Integer getAvg_status() {
        return avg_status;
    }

    public void setAvg_status(Integer avg_status) {
        this.avg_status = avg_status;
    }

    public Long getArea_id() {
        return area_id;
    }

    public void setArea_id(Long area_id) {
        this.area_id = area_id;
    }

    public Long getUser_prov_customer_id() {
        return user_prov_customer_id;
    }

    public void setUser_prov_customer_id(Long user_prov_customer_id) {
        this.user_prov_customer_id = user_prov_customer_id;
    }

    public Long getAccountWayId() {
        return accountWayId;
    }

    public void setAccountWayId(Long accountWayId) {
        this.accountWayId = accountWayId;
    }

    public Integer getLock_status() {
        return lock_status;
    }

    public void setLock_status(Integer lock_status) {
        this.lock_status = lock_status;
    }

    public BigDecimal getCenter_rec_fee() {
        return center_rec_fee;
    }

    public void setCenter_rec_fee(BigDecimal center_rec_fee) {
        this.center_rec_fee = center_rec_fee;
    }

    public Integer getCenter_rec_count() {
        return center_rec_count;
    }

    public void setCenter_rec_count(Integer center_rec_count) {
        this.center_rec_count = center_rec_count;
    }

    public String getDispatch() {
        return dispatch;
    }

    public void setDispatch(String dispatch) {
        this.dispatch = dispatch;
    }

    public BigDecimal getDisp_weight() {
        return disp_weight;
    }

    public void setDisp_weight(BigDecimal disp_weight) {
        this.disp_weight = disp_weight;
    }

    public String getCenter_scan_man_code() {
        return center_scan_man_code;
    }

    public void setCenter_scan_man_code(String center_scan_man_code) {
        this.center_scan_man_code = center_scan_man_code;
    }

    public BigDecimal getSurface_fee() {
        return surface_fee;
    }

    public void setSurface_fee(BigDecimal surface_fee) {
        this.surface_fee = surface_fee;
    }

    public BigDecimal getGross_fee() {
        return gross_fee;
    }

    public void setGross_fee(BigDecimal gross_fee) {
        this.gross_fee = gross_fee;
    }

    public BigDecimal getGoods_payment() {
        return goods_payment;
    }

    public void setGoods_payment(BigDecimal goods_payment) {
        this.goods_payment = goods_payment;
    }

    public BigDecimal getDi_fee() {
        return di_fee;
    }

    public void setDi_fee(BigDecimal di_fee) {
        this.di_fee = di_fee;
    }

    public BigDecimal getCod_fee() {
        return cod_fee;
    }

    public void setCod_fee(BigDecimal cod_fee) {
        this.cod_fee = cod_fee;
    }

    public BigDecimal getCod_payment() {
        return cod_payment;
    }

    public void setCod_payment(BigDecimal cod_payment) {
        this.cod_payment = cod_payment;
    }

    public Integer getDate_source() {
        return date_source;
    }

    public void setDate_source(Integer date_source) {
        this.date_source = date_source;
    }

    public String getGmt_modifie() {
        return gmt_modifie;
    }

    public void setGmt_modifie(String gmt_modifie) {
        this.gmt_modifie = gmt_modifie;
    }

    public String getEditor_name() {
        return editor_name;
    }

    public void setEditor_name(String editor_name) {
        this.editor_name = editor_name;
    }

    public String getGmt_create() {
        return gmt_create;
    }

    public void setGmt_create(String gmt_create) {
        this.gmt_create = gmt_create;
    }

    public String getProceeds_man() {
        return proceeds_man;
    }

    public void setProceeds_man(String proceeds_man) {
        this.proceeds_man = proceeds_man;
    }

    public String getProceeds_time() {
        return proceeds_time;
    }

    public void setProceeds_time(String proceeds_time) {
        this.proceeds_time = proceeds_time;
    }

    public String getHexiao_man() {
        return hexiao_man;
    }

    public void setHexiao_man(String hexiao_man) {
        this.hexiao_man = hexiao_man;
    }

    public String getHexiao_time() {
        return hexiao_time;
    }

    public void setHexiao_time(String hexiao_time) {
        this.hexiao_time = hexiao_time;
    }

    public Long getAccount_user_id() {
        return account_user_id;
    }

    public void setAccount_user_id(Long account_user_id) {
        this.account_user_id = account_user_id;
    }

    public String getGmt_modified() {
        return gmt_modified;
    }

    public void setGmt_modified(String gmt_modified) {
        this.gmt_modified = gmt_modified;
    }

    public BigDecimal getCustomer_bill_fee() {
        return customer_bill_fee;
    }

    public void setCustomer_bill_fee(BigDecimal customer_bill_fee) {
        this.customer_bill_fee = customer_bill_fee;
    }

    public BigDecimal getCustomer_add_fee() {
        return customer_add_fee;
    }

    public void setCustomer_add_fee(BigDecimal customer_add_fee) {
        this.customer_add_fee = customer_add_fee;
    }

    public String getUser_prov_customer_name() {
        return user_prov_customer_name;
    }

    public void setUser_prov_customer_name(String user_prov_customer_name) {
        this.user_prov_customer_name = user_prov_customer_name;
    }

    public Long getCity_id() {
        return city_id;
    }

    public void setCity_id(Long city_id) {
        this.city_id = city_id;
    }

    public String getLock_man() {
        return lock_man;
    }

    public void setLock_man(String lock_man) {
        this.lock_man = lock_man;
    }

    public String getLock_time() {
        return lock_time;
    }

    public void setLock_time(String lock_time) {
        this.lock_time = lock_time;
    }

    public Integer getDispatch_type() {
        return dispatch_type;
    }

    public void setDispatch_type(Integer dispatch_type) {
        this.dispatch_type = dispatch_type;
    }

    public Integer getOrder_type() {
        return order_type;
    }

    public void setOrder_type(Integer order_type) {
        this.order_type = order_type;
    }

    public BigDecimal getTwo_level_fee() {
        return two_level_fee;
    }

    public void setTwo_level_fee(BigDecimal two_level_fee) {
        this.two_level_fee = two_level_fee;
    }

    public BigDecimal getBill_subsidy() {
        return bill_subsidy;
    }

    public void setBill_subsidy(BigDecimal bill_subsidy) {
        this.bill_subsidy = bill_subsidy;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
