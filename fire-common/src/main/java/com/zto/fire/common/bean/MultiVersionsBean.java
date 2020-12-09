package com.zto.fire.common.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.zto.fire.common.anno.FieldName;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.BigDecimalConverter;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 多版本HBase实体Bean
 * Created by ChengLong on 2017-08-17.
 */
public class MultiVersionsBean extends HBaseBaseBean<MultiVersionsBean> {
    @FieldName("multiFields")
    private String multiFields;

    @FieldName(value = "HBaseBaseBean", disuse = true)
    private HBaseBaseBean<?> target;

    @FieldName(value = "BIGDECIMAL_ZERO", disuse = true)
    private static final BigDecimal BIGDECIMAL_ZERO = new BigDecimal("0");

    static {
        // 这里一定要注册默认值，使用null也可以
        BigDecimalConverter bd = new BigDecimalConverter(BIGDECIMAL_ZERO);
        ConvertUtils.register(bd, java.math.BigDecimal.class);
    }

    public String getMultiFields() {
        return multiFields;
    }

    public void setMultiFields(String multiFields) {
        this.multiFields = multiFields;
    }

    public HBaseBaseBean getTarget() {
        return target;
    }

    public void setTarget(HBaseBaseBean<?> target) {
        this.target = target;
    }

    public MultiVersionsBean(HBaseBaseBean<?> target) {
        this.target = (HBaseBaseBean) target.buildRowKey();
        this.multiFields = JSON.toJSONString(this.target, SerializerFeature.WriteMapNullValue);
    }

    public MultiVersionsBean() {

    }

    @Override
    public MultiVersionsBean buildRowKey() {
        try {
            if(this.target == null && StringUtils.isNotBlank(this.multiFields)) {
                Map<String, String> map = JSON.parseObject(this.multiFields, Map.class);
                Class<?> clazz = Class.forName(map.get("className"));
                HBaseBaseBean<?> bean = (HBaseBaseBean) clazz.newInstance();
                BeanUtils.populate(bean, map);
                this.target = (HBaseBaseBean) bean.buildRowKey();
            }

            if (this.target != null) {
                this.target = (HBaseBaseBean) this.target.buildRowKey();
                this.rowKey = this.target.rowKey;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return this;
    }
}
