package com.zto.bigdata.spark.bean;

import com.zto.bigdata.spark.common.anno.FieldName;
import com.zto.bigdata.spark.common.bean.HBaseBaseBean;
import org.apache.commons.lang3.StringUtils;

public class MainOrder extends HBaseBaseBean<MainOrder> {
    @FieldName("gtid")
    private String gtid;
    @FieldName("logFile")
    private String logFile;
    @FieldName("offset")
    private String offset;
    @FieldName("op_type")
    private String op_type;
    @FieldName("pos")
    private Long pos;
    @FieldName("schema")
    private String schema;
    @FieldName("table")
    private String table;
    @FieldName("msg_when")
    private Long msg_when;
    @FieldName("assign_emp")
    private String assign_emp;
    @FieldName("assign_emp_code")
    private String assign_emp_code;
    @FieldName("assign_site")
    private String assign_site;
    @FieldName("assign_site_code")
    private String assign_site_code;
    @FieldName("bill_code")
    private String bill_code;
    @FieldName("bill_sign_site_code")
    private String bill_sign_site_code;
    @FieldName("bill_sign_site_date")
    private String bill_sign_site_date;
    @FieldName("bill_status")
    private Long bill_status;
    @FieldName("creator")
    private String creator;
    @FieldName("customer_code")
    private String customer_code;
    @FieldName("disp_emp")
    private String disp_emp;
    @FieldName("disp_emp_code")
    private String disp_emp_code;
    @FieldName("disp_emp_date")
    private String disp_emp_date;
    @FieldName("disp_site")
    private String disp_site;
    @FieldName("disp_site_date")
    private String disp_site_date;
    @FieldName("disp_site_id")
    private String disp_site_id;
    @FieldName("extra_info")
    private String extra_info;
    @FieldName("forecast_disp_site_code")
    private String forecast_disp_site_code;
    @FieldName("forecast_rec_site_code")
    private String forecast_rec_site_code;
    @FieldName("fst_code")
    private String fst_code;
    @FieldName("gmt_created")
    private String gmt_created;
    @FieldName("gmt_modified")
    private String gmt_modified;
    @FieldName("has_vas")
    private Long has_vas;
    @FieldName("id")
    private Long id;
    @FieldName("is_decipher")
    private Long is_decipher;
    @FieldName("is_deleted")
    private String is_deleted;
    @FieldName("is_prob")
    private Long is_prob;
    @FieldName("is_reject")
    private Long is_reject;
    @FieldName("is_repeat")
    private Long is_repeat;
    @FieldName("modifier")
    private String modifier;
    @FieldName("old_order_code")
    private String old_order_code;
    @FieldName("opt_done")
    private String opt_done;
    @FieldName("opt_isbooking")
    private String opt_isbooking;
    @FieldName("opt_print_status")
    private Long opt_print_status;
    @FieldName("opt_reminder")
    private String opt_reminder;
    @FieldName("opt_visit")
    private String opt_visit;
    @FieldName("order_code")
    private Long order_code;
    @FieldName("order_create_date")
    private String order_create_date;
    @FieldName("order_item_name")
    private String order_item_name;
    @FieldName("order_pay_type")
    private String order_pay_type;
    @FieldName("order_remark")
    private String order_remark;
    @FieldName("order_service_type")
    private Long order_service_type;
    @FieldName("order_src")
    private Long order_src;
    @FieldName("order_status")
    private Long order_status;
    @FieldName("order_type")
    private Long order_type;
    @FieldName("package_site")
    private String package_site;
    @FieldName("parcel_freight")
    private Long parcel_freight;
    @FieldName("parcel_order_sum")
    private Long parcel_order_sum;
    @FieldName("parcel_other_fee")
    private Long parcel_other_fee;
    @FieldName("parcel_packing_fee")
    private Long parcel_packing_fee;
    @FieldName("parcel_premium")
    private Long parcel_premium;
    @FieldName("parcel_price")
    private Long parcel_price;
    @FieldName("parcel_quantity")
    private Long parcel_quantity;
    @FieldName("parcel_size")
    private String parcel_size;
    @FieldName("parcel_taking_end_time")
    private String parcel_taking_end_time;
    @FieldName("parcel_taking_start_time")
    private String parcel_taking_start_time;
    @FieldName("parcel_weight")
    private Long parcel_weight;
    @FieldName("partner_id")
    private String partner_id;
    @FieldName("partner_order_code")
    private String partner_order_code;
    @FieldName("partner_trade_id")
    private String partner_trade_id;
    @FieldName("pre_order_status")
    private Long pre_order_status;
    @FieldName("product_id")
    private String product_id;
    @FieldName("rec_emp")
    private String rec_emp;
    @FieldName("rec_emp_code")
    private String rec_emp_code;
    @FieldName("rec_emp_date")
    private String rec_emp_date;
    @FieldName("rec_site")
    private String rec_site;
    @FieldName("rec_site_code")
    private String rec_site_code;
    @FieldName("rec_site_date")
    private String rec_site_date;
    @FieldName("receiv_address")
    private String receiv_address;
    @FieldName("receiv_ao")
    private String receiv_ao;
    @FieldName("receiv_ao_id")
    private String receiv_ao_id;
    @FieldName("receiv_ao_lat")
    private String receiv_ao_lat;
    @FieldName("receiv_ao_lng")
    private String receiv_ao_lng;
    @FieldName("receiv_city")
    private String receiv_city;
    @FieldName("receiv_city_id")
    private String receiv_city_id;
    @FieldName("receiv_company")
    private String receiv_company;
    @FieldName("receiv_county")
    private String receiv_county;
    @FieldName("receiv_county_id")
    private String receiv_county_id;
    @FieldName("receiv_id")
    private String receiv_id;
    @FieldName("receiv_mobile")
    private String receiv_mobile;
    @FieldName("receiv_name")
    private String receiv_name;
    @FieldName("receiv_phone")
    private String receiv_phone;
    @FieldName("receiv_prov")
    private String receiv_prov;
    @FieldName("receiv_prov_id")
    private String receiv_prov_id;
    @FieldName("receiv_state")
    private String receiv_state;
    @FieldName("receiv_state_id")
    private String receiv_state_id;
    @FieldName("receiv_zipcode")
    private String receiv_zipcode;
    @FieldName("send_address")
    private String send_address;
    @FieldName("send_ao")
    private String send_ao;
    @FieldName("send_ao_id")
    private String send_ao_id;
    @FieldName("send_ao_lat")
    private String send_ao_lat;
    @FieldName("send_ao_lng")
    private String send_ao_lng;
    @FieldName("send_city")
    private String send_city;
    @FieldName("send_city_id")
    private String send_city_id;
    @FieldName("send_company")
    private String send_company;
    @FieldName("send_county")
    private String send_county;
    @FieldName("send_county_id")
    private String send_county_id;
    @FieldName("send_id")
    private String send_id;
    @FieldName("send_mobile")
    private String send_mobile;
    @FieldName("send_name")
    private String send_name;
    @FieldName("send_phone")
    private String send_phone;
    @FieldName("send_prov")
    private String send_prov;
    @FieldName("send_prov_id")
    private String send_prov_id;
    @FieldName("send_state")
    private String send_state;
    @FieldName("send_state_id")
    private String send_state_id;
    @FieldName("send_zipcode")
    private String send_zipcode;
    @FieldName("sign_emp")
    private String sign_emp;
    @FieldName("sign_emp_code")
    private String sign_emp_code;
    @FieldName("sign_emp_date")
    private String sign_emp_date;
    @FieldName("sign_site")
    private String sign_site;
    @FieldName("sign_site_date")
    private String sign_site_date;
    @FieldName("sign_site_id")
    private String sign_site_id;
    @FieldName("snd_code")
    private String snd_code;
    @FieldName("taking_emp")
    private String taking_emp;
    @FieldName("taking_emp_code")
    private String taking_emp_code;
    @FieldName("taking_emp_date")
    private String taking_emp_date;
    @FieldName("taking_site")
    private String taking_site;
    @FieldName("taking_site_code")
    private String taking_site_code;
    @FieldName("taking_site_date")
    private String taking_site_date;
    @FieldName("trd_code")
    private String trd_code;
    @FieldName("user_id")
    private String user_id;
    @FieldName("vas_collect_currency")
    private String vas_collect_currency;
    @FieldName("vas_collect_sum")
    private Long vas_collect_sum;
    @FieldName("vericode")
    private String vericode;
    @FieldName("version")
    private Long version;
    @FieldName("before_bill_code")
    private String before_bill_code;
    @FieldName("before_order_code")
    private Long before_order_code;

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public String getLogFile() {
        return logFile;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public String getOp_type() {
        return op_type;
    }

    public void setOp_type(String op_type) {
        this.op_type = op_type;
    }

    public Long getPos() {
        return pos;
    }

    public void setPos(Long pos) {
        this.pos = pos;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Long getMsg_when() {
        return msg_when;
    }

    public void setMsg_when(Long msg_when) {
        this.msg_when = msg_when;
    }

    public String getAssign_emp() {
        return assign_emp;
    }

    public void setAssign_emp(String assign_emp) {
        this.assign_emp = assign_emp;
    }

    public String getAssign_emp_code() {
        return assign_emp_code;
    }

    public void setAssign_emp_code(String assign_emp_code) {
        this.assign_emp_code = assign_emp_code;
    }

    public String getAssign_site() {
        return assign_site;
    }

    public void setAssign_site(String assign_site) {
        this.assign_site = assign_site;
    }

    public String getAssign_site_code() {
        return assign_site_code;
    }

    public void setAssign_site_code(String assign_site_code) {
        this.assign_site_code = assign_site_code;
    }

    public String getBill_code() {
        return bill_code;
    }

    public void setBill_code(String bill_code) {
        this.bill_code = bill_code;
    }

    public String getBill_sign_site_code() {
        return bill_sign_site_code;
    }

    public void setBill_sign_site_code(String bill_sign_site_code) {
        this.bill_sign_site_code = bill_sign_site_code;
    }

    public String getBill_sign_site_date() {
        return bill_sign_site_date;
    }

    public void setBill_sign_site_date(String bill_sign_site_date) {
        this.bill_sign_site_date = bill_sign_site_date;
    }

    public Long getBill_status() {
        return bill_status;
    }

    public void setBill_status(Long bill_status) {
        this.bill_status = bill_status;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getCustomer_code() {
        return customer_code;
    }

    public void setCustomer_code(String customer_code) {
        this.customer_code = customer_code;
    }

    public String getDisp_emp() {
        return disp_emp;
    }

    public void setDisp_emp(String disp_emp) {
        this.disp_emp = disp_emp;
    }

    public String getDisp_emp_code() {
        return disp_emp_code;
    }

    public void setDisp_emp_code(String disp_emp_code) {
        this.disp_emp_code = disp_emp_code;
    }

    public String getDisp_emp_date() {
        return disp_emp_date;
    }

    public void setDisp_emp_date(String disp_emp_date) {
        this.disp_emp_date = disp_emp_date;
    }

    public String getDisp_site() {
        return disp_site;
    }

    public void setDisp_site(String disp_site) {
        this.disp_site = disp_site;
    }

    public String getDisp_site_date() {
        return disp_site_date;
    }

    public void setDisp_site_date(String disp_site_date) {
        this.disp_site_date = disp_site_date;
    }

    public String getDisp_site_id() {
        return disp_site_id;
    }

    public void setDisp_site_id(String disp_site_id) {
        this.disp_site_id = disp_site_id;
    }

    public String getExtra_info() {
        return extra_info;
    }

    public void setExtra_info(String extra_info) {
        this.extra_info = extra_info;
    }

    public String getForecast_disp_site_code() {
        return forecast_disp_site_code;
    }

    public void setForecast_disp_site_code(String forecast_disp_site_code) {
        this.forecast_disp_site_code = forecast_disp_site_code;
    }

    public String getForecast_rec_site_code() {
        return forecast_rec_site_code;
    }

    public void setForecast_rec_site_code(String forecast_rec_site_code) {
        this.forecast_rec_site_code = forecast_rec_site_code;
    }

    public String getFst_code() {
        return fst_code;
    }

    public void setFst_code(String fst_code) {
        this.fst_code = fst_code;
    }

    public String getGmt_created() {
        return gmt_created;
    }

    public void setGmt_created(String gmt_created) {
        this.gmt_created = gmt_created;
    }

    public String getGmt_modified() {
        return gmt_modified;
    }

    public void setGmt_modified(String gmt_modified) {
        this.gmt_modified = gmt_modified;
    }

    public Long getHas_vas() {
        return has_vas;
    }

    public void setHas_vas(Long has_vas) {
        this.has_vas = has_vas;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getIs_decipher() {
        return is_decipher;
    }

    public void setIs_decipher(Long is_decipher) {
        this.is_decipher = is_decipher;
    }

    public String getIs_deleted() {
        return is_deleted;
    }

    public void setIs_deleted(String is_deleted) {
        this.is_deleted = is_deleted;
    }

    public Long getIs_prob() {
        return is_prob;
    }

    public void setIs_prob(Long is_prob) {
        this.is_prob = is_prob;
    }

    public Long getIs_reject() {
        return is_reject;
    }

    public void setIs_reject(Long is_reject) {
        this.is_reject = is_reject;
    }

    public Long getIs_repeat() {
        return is_repeat;
    }

    public void setIs_repeat(Long is_repeat) {
        this.is_repeat = is_repeat;
    }

    public String getModifier() {
        return modifier;
    }

    public void setModifier(String modifier) {
        this.modifier = modifier;
    }

    public String getOld_order_code() {
        return old_order_code;
    }

    public void setOld_order_code(String old_order_code) {
        this.old_order_code = old_order_code;
    }

    public String getOpt_done() {
        return opt_done;
    }

    public void setOpt_done(String opt_done) {
        this.opt_done = opt_done;
    }

    public String getOpt_isbooking() {
        return opt_isbooking;
    }

    public void setOpt_isbooking(String opt_isbooking) {
        this.opt_isbooking = opt_isbooking;
    }

    public Long getOpt_print_status() {
        return opt_print_status;
    }

    public void setOpt_print_status(Long opt_print_status) {
        this.opt_print_status = opt_print_status;
    }

    public String getOpt_reminder() {
        return opt_reminder;
    }

    public void setOpt_reminder(String opt_reminder) {
        this.opt_reminder = opt_reminder;
    }

    public String getOpt_visit() {
        return opt_visit;
    }

    public void setOpt_visit(String opt_visit) {
        this.opt_visit = opt_visit;
    }

    public Long getOrder_code() {
        return order_code;
    }

    public void setOrder_code(Long order_code) {
        this.order_code = order_code;
    }

    public String getOrder_create_date() {
        return order_create_date;
    }

    public void setOrder_create_date(String order_create_date) {
        this.order_create_date = order_create_date;
    }

    public String getOrder_item_name() {
        return order_item_name;
    }

    public void setOrder_item_name(String order_item_name) {
        this.order_item_name = order_item_name;
    }

    public String getOrder_pay_type() {
        return order_pay_type;
    }

    public void setOrder_pay_type(String order_pay_type) {
        this.order_pay_type = order_pay_type;
    }

    public String getOrder_remark() {
        return order_remark;
    }

    public void setOrder_remark(String order_remark) {
        this.order_remark = order_remark;
    }

    public Long getOrder_service_type() {
        return order_service_type;
    }

    public void setOrder_service_type(Long order_service_type) {
        this.order_service_type = order_service_type;
    }

    public Long getOrder_src() {
        return order_src;
    }

    public void setOrder_src(Long order_src) {
        this.order_src = order_src;
    }

    public Long getOrder_status() {
        return order_status;
    }

    public void setOrder_status(Long order_status) {
        this.order_status = order_status;
    }

    public Long getOrder_type() {
        return order_type;
    }

    public void setOrder_type(Long order_type) {
        this.order_type = order_type;
    }

    public String getPackage_site() {
        return package_site;
    }

    public void setPackage_site(String package_site) {
        this.package_site = package_site;
    }

    public Long getParcel_freight() {
        return parcel_freight;
    }

    public void setParcel_freight(Long parcel_freight) {
        this.parcel_freight = parcel_freight;
    }

    public Long getParcel_order_sum() {
        return parcel_order_sum;
    }

    public void setParcel_order_sum(Long parcel_order_sum) {
        this.parcel_order_sum = parcel_order_sum;
    }

    public Long getParcel_other_fee() {
        return parcel_other_fee;
    }

    public void setParcel_other_fee(Long parcel_other_fee) {
        this.parcel_other_fee = parcel_other_fee;
    }

    public Long getParcel_packing_fee() {
        return parcel_packing_fee;
    }

    public void setParcel_packing_fee(Long parcel_packing_fee) {
        this.parcel_packing_fee = parcel_packing_fee;
    }

    public Long getParcel_premium() {
        return parcel_premium;
    }

    public void setParcel_premium(Long parcel_premium) {
        this.parcel_premium = parcel_premium;
    }

    public Long getParcel_price() {
        return parcel_price;
    }

    public void setParcel_price(Long parcel_price) {
        this.parcel_price = parcel_price;
    }

    public Long getParcel_quantity() {
        return parcel_quantity;
    }

    public void setParcel_quantity(Long parcel_quantity) {
        this.parcel_quantity = parcel_quantity;
    }

    public String getParcel_size() {
        return parcel_size;
    }

    public void setParcel_size(String parcel_size) {
        this.parcel_size = parcel_size;
    }

    public String getParcel_taking_end_time() {
        return parcel_taking_end_time;
    }

    public void setParcel_taking_end_time(String parcel_taking_end_time) {
        this.parcel_taking_end_time = parcel_taking_end_time;
    }

    public String getParcel_taking_start_time() {
        return parcel_taking_start_time;
    }

    public void setParcel_taking_start_time(String parcel_taking_start_time) {
        this.parcel_taking_start_time = parcel_taking_start_time;
    }

    public Long getParcel_weight() {
        return parcel_weight;
    }

    public void setParcel_weight(Long parcel_weight) {
        this.parcel_weight = parcel_weight;
    }

    public String getPartner_id() {
        return partner_id;
    }

    public void setPartner_id(String partner_id) {
        this.partner_id = partner_id;
    }

    public String getPartner_order_code() {
        return partner_order_code;
    }

    public void setPartner_order_code(String partner_order_code) {
        this.partner_order_code = partner_order_code;
    }

    public String getPartner_trade_id() {
        return partner_trade_id;
    }

    public void setPartner_trade_id(String partner_trade_id) {
        this.partner_trade_id = partner_trade_id;
    }

    public Long getPre_order_status() {
        return pre_order_status;
    }

    public void setPre_order_status(Long pre_order_status) {
        this.pre_order_status = pre_order_status;
    }

    public String getProduct_id() {
        return product_id;
    }

    public void setProduct_id(String product_id) {
        this.product_id = product_id;
    }

    public String getRec_emp() {
        return rec_emp;
    }

    public void setRec_emp(String rec_emp) {
        this.rec_emp = rec_emp;
    }

    public String getRec_emp_code() {
        return rec_emp_code;
    }

    public void setRec_emp_code(String rec_emp_code) {
        this.rec_emp_code = rec_emp_code;
    }

    public String getRec_emp_date() {
        return rec_emp_date;
    }

    public void setRec_emp_date(String rec_emp_date) {
        this.rec_emp_date = rec_emp_date;
    }

    public String getRec_site() {
        return rec_site;
    }

    public void setRec_site(String rec_site) {
        this.rec_site = rec_site;
    }

    public String getRec_site_code() {
        return rec_site_code;
    }

    public void setRec_site_code(String rec_site_code) {
        this.rec_site_code = rec_site_code;
    }

    public String getRec_site_date() {
        return rec_site_date;
    }

    public void setRec_site_date(String rec_site_date) {
        this.rec_site_date = rec_site_date;
    }

    public String getReceiv_address() {
        return receiv_address;
    }

    public void setReceiv_address(String receiv_address) {
        this.receiv_address = receiv_address;
    }

    public String getReceiv_ao() {
        return receiv_ao;
    }

    public void setReceiv_ao(String receiv_ao) {
        this.receiv_ao = receiv_ao;
    }

    public String getReceiv_ao_id() {
        return receiv_ao_id;
    }

    public void setReceiv_ao_id(String receiv_ao_id) {
        this.receiv_ao_id = receiv_ao_id;
    }

    public String getReceiv_ao_lat() {
        return receiv_ao_lat;
    }

    public void setReceiv_ao_lat(String receiv_ao_lat) {
        this.receiv_ao_lat = receiv_ao_lat;
    }

    public String getReceiv_ao_lng() {
        return receiv_ao_lng;
    }

    public void setReceiv_ao_lng(String receiv_ao_lng) {
        this.receiv_ao_lng = receiv_ao_lng;
    }

    public String getReceiv_city() {
        return receiv_city;
    }

    public void setReceiv_city(String receiv_city) {
        this.receiv_city = receiv_city;
    }

    public String getReceiv_city_id() {
        return receiv_city_id;
    }

    public void setReceiv_city_id(String receiv_city_id) {
        this.receiv_city_id = receiv_city_id;
    }

    public String getReceiv_company() {
        return receiv_company;
    }

    public void setReceiv_company(String receiv_company) {
        this.receiv_company = receiv_company;
    }

    public String getReceiv_county() {
        return receiv_county;
    }

    public void setReceiv_county(String receiv_county) {
        this.receiv_county = receiv_county;
    }

    public String getReceiv_county_id() {
        return receiv_county_id;
    }

    public void setReceiv_county_id(String receiv_county_id) {
        this.receiv_county_id = receiv_county_id;
    }

    public String getReceiv_id() {
        return receiv_id;
    }

    public void setReceiv_id(String receiv_id) {
        this.receiv_id = receiv_id;
    }

    public String getReceiv_mobile() {
        return receiv_mobile;
    }

    public void setReceiv_mobile(String receiv_mobile) {
        this.receiv_mobile = receiv_mobile;
    }

    public String getReceiv_name() {
        return receiv_name;
    }

    public void setReceiv_name(String receiv_name) {
        this.receiv_name = receiv_name;
    }

    public String getReceiv_phone() {
        return receiv_phone;
    }

    public void setReceiv_phone(String receiv_phone) {
        this.receiv_phone = receiv_phone;
    }

    public String getReceiv_prov() {
        return receiv_prov;
    }

    public void setReceiv_prov(String receiv_prov) {
        this.receiv_prov = receiv_prov;
    }

    public String getReceiv_prov_id() {
        return receiv_prov_id;
    }

    public void setReceiv_prov_id(String receiv_prov_id) {
        this.receiv_prov_id = receiv_prov_id;
    }

    public String getReceiv_state() {
        return receiv_state;
    }

    public void setReceiv_state(String receiv_state) {
        this.receiv_state = receiv_state;
    }

    public String getReceiv_state_id() {
        return receiv_state_id;
    }

    public void setReceiv_state_id(String receiv_state_id) {
        this.receiv_state_id = receiv_state_id;
    }

    public String getReceiv_zipcode() {
        return receiv_zipcode;
    }

    public void setReceiv_zipcode(String receiv_zipcode) {
        this.receiv_zipcode = receiv_zipcode;
    }

    public String getSend_address() {
        return send_address;
    }

    public void setSend_address(String send_address) {
        this.send_address = send_address;
    }

    public String getSend_ao() {
        return send_ao;
    }

    public void setSend_ao(String send_ao) {
        this.send_ao = send_ao;
    }

    public String getSend_ao_id() {
        return send_ao_id;
    }

    public void setSend_ao_id(String send_ao_id) {
        this.send_ao_id = send_ao_id;
    }

    public String getSend_ao_lat() {
        return send_ao_lat;
    }

    public void setSend_ao_lat(String send_ao_lat) {
        this.send_ao_lat = send_ao_lat;
    }

    public String getSend_ao_lng() {
        return send_ao_lng;
    }

    public void setSend_ao_lng(String send_ao_lng) {
        this.send_ao_lng = send_ao_lng;
    }

    public String getSend_city() {
        return send_city;
    }

    public void setSend_city(String send_city) {
        this.send_city = send_city;
    }

    public String getSend_city_id() {
        return send_city_id;
    }

    public void setSend_city_id(String send_city_id) {
        this.send_city_id = send_city_id;
    }

    public String getSend_company() {
        return send_company;
    }

    public void setSend_company(String send_company) {
        this.send_company = send_company;
    }

    public String getSend_county() {
        return send_county;
    }

    public void setSend_county(String send_county) {
        this.send_county = send_county;
    }

    public String getSend_county_id() {
        return send_county_id;
    }

    public void setSend_county_id(String send_county_id) {
        this.send_county_id = send_county_id;
    }

    public String getSend_id() {
        return send_id;
    }

    public void setSend_id(String send_id) {
        this.send_id = send_id;
    }

    public String getSend_mobile() {
        return send_mobile;
    }

    public void setSend_mobile(String send_mobile) {
        this.send_mobile = send_mobile;
    }

    public String getSend_name() {
        return send_name;
    }

    public void setSend_name(String send_name) {
        this.send_name = send_name;
    }

    public String getSend_phone() {
        return send_phone;
    }

    public void setSend_phone(String send_phone) {
        this.send_phone = send_phone;
    }

    public String getSend_prov() {
        return send_prov;
    }

    public void setSend_prov(String send_prov) {
        this.send_prov = send_prov;
    }

    public String getSend_prov_id() {
        return send_prov_id;
    }

    public void setSend_prov_id(String send_prov_id) {
        this.send_prov_id = send_prov_id;
    }

    public String getSend_state() {
        return send_state;
    }

    public void setSend_state(String send_state) {
        this.send_state = send_state;
    }

    public String getSend_state_id() {
        return send_state_id;
    }

    public void setSend_state_id(String send_state_id) {
        this.send_state_id = send_state_id;
    }

    public String getSend_zipcode() {
        return send_zipcode;
    }

    public void setSend_zipcode(String send_zipcode) {
        this.send_zipcode = send_zipcode;
    }

    public String getSign_emp() {
        return sign_emp;
    }

    public void setSign_emp(String sign_emp) {
        this.sign_emp = sign_emp;
    }

    public String getSign_emp_code() {
        return sign_emp_code;
    }

    public void setSign_emp_code(String sign_emp_code) {
        this.sign_emp_code = sign_emp_code;
    }

    public String getSign_emp_date() {
        return sign_emp_date;
    }

    public void setSign_emp_date(String sign_emp_date) {
        this.sign_emp_date = sign_emp_date;
    }

    public String getSign_site() {
        return sign_site;
    }

    public void setSign_site(String sign_site) {
        this.sign_site = sign_site;
    }

    public String getSign_site_date() {
        return sign_site_date;
    }

    public void setSign_site_date(String sign_site_date) {
        this.sign_site_date = sign_site_date;
    }

    public String getSign_site_id() {
        return sign_site_id;
    }

    public void setSign_site_id(String sign_site_id) {
        this.sign_site_id = sign_site_id;
    }

    public String getSnd_code() {
        return snd_code;
    }

    public void setSnd_code(String snd_code) {
        this.snd_code = snd_code;
    }

    public String getTaking_emp() {
        return taking_emp;
    }

    public void setTaking_emp(String taking_emp) {
        this.taking_emp = taking_emp;
    }

    public String getTaking_emp_code() {
        return taking_emp_code;
    }

    public void setTaking_emp_code(String taking_emp_code) {
        this.taking_emp_code = taking_emp_code;
    }

    public String getTaking_emp_date() {
        return taking_emp_date;
    }

    public void setTaking_emp_date(String taking_emp_date) {
        this.taking_emp_date = taking_emp_date;
    }

    public String getTaking_site() {
        return taking_site;
    }

    public void setTaking_site(String taking_site) {
        this.taking_site = taking_site;
    }

    public String getTaking_site_code() {
        return taking_site_code;
    }

    public void setTaking_site_code(String taking_site_code) {
        this.taking_site_code = taking_site_code;
    }

    public String getTaking_site_date() {
        return taking_site_date;
    }

    public void setTaking_site_date(String taking_site_date) {
        this.taking_site_date = taking_site_date;
    }

    public String getTrd_code() {
        return trd_code;
    }

    public void setTrd_code(String trd_code) {
        this.trd_code = trd_code;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getVas_collect_currency() {
        return vas_collect_currency;
    }

    public void setVas_collect_currency(String vas_collect_currency) {
        this.vas_collect_currency = vas_collect_currency;
    }

    public Long getVas_collect_sum() {
        return vas_collect_sum;
    }

    public void setVas_collect_sum(Long vas_collect_sum) {
        this.vas_collect_sum = vas_collect_sum;
    }

    public String getVericode() {
        return vericode;
    }

    public void setVericode(String vericode) {
        this.vericode = vericode;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public String getBefore_bill_code() {
        return before_bill_code;
    }

    public void setBefore_bill_code(String before_bill_code) {
        this.before_bill_code = before_bill_code;
    }

    public Long getBefore_order_code() {
        return before_order_code;
    }

    public void setBefore_order_code(Long before_order_code) {
        this.before_order_code = before_order_code;
    }

    @Override
    public MainOrder buildRowKey() {
        this.rowKey = (StringUtils.reverse(this.bill_code) + "00000000000000000000").substring(0, 20);
        return this;
    }
}
