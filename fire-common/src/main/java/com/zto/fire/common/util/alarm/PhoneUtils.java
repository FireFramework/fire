package com.zto.fire.common.util.alarm;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.zto.fire.common.bean.VoiceInfo;
import com.zto.fire.common.util.EncryptUtils;
import com.zto.fire.common.util.HttpClientUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;


/**
 * 电话告警工具
 *
 * @author ChengLong 2019-9-4 17:00:11
 */
public class PhoneUtils {
    private static final String appId = "datacloudMonitor";
    private static final String appVer = "1.0.0.0";
    private static final String appKey = "CB9Z9HM20A9PM1ZMHU2DLIU0QIVRPGIX";
    private static final String methodSms = "sms.send.company";
    private static final String siteCode = "02100";
    private static final String voiceUrl = "http://apipf.ztosys.com/gateway";
    private static final String smsUrl = "http://apipf.ztosys.com/gateway";
    private static final String accountCode = "02100.3863";


    /**
     * 发送短信告警
     *
     * @param telephone 电话号码
     * @param message   告警内容
     */
    public static void sendSms(String telephone, String message) {
        String appTimestamp = System.currentTimeMillis() + "";
        if (telephone != null && !"".equals(telephone)) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("AppID", appId);
            map.put("AppVer", appVer);
            String appNotic = UUID.randomUUID().toString().replaceAll("-", "");
            map.put("AppNotic", appNotic);
            map.put("AppTimestamp", appTimestamp);
            map.put("AppSign", md5AppAsign(appId, appKey, appNotic, appTimestamp));
            map.put("method", methodSms);
            Map<String, String> dataMap = new HashMap<String, String>();
            map.put("Data", dataMap);
            dataMap.put("SiteCode", siteCode);
            dataMap.put("Mobiles", telephone);
            dataMap.put("Message", message);
            dataMap.put("IsHotline", "false");
            dataMap.put("IsConfidential", "false");
            dataMap.put("ReplyTopicID", "0");
            dataMap.put("ReqSerial", UUID.randomUUID().toString().replaceAll("-", ""));
            HttpClientUtils.doPostIgnore(smsUrl, JSON.toJSONString(map));
        }
    }

    /**
     * 发送语音告警
     *
     * @param telephone 电话号码
     * @param message   告警内容
     */
    public static void sendVoice(String telephone, String message) {
        HashMap<String, String> voiceMap = new HashMap<>();
        voiceMap.put("telephones", telephone);
        voiceMap.put("message", message);
        voiceMap.put("appId", appId);
        voiceMap.put("appVer", "1.0.0.0");
        voiceMap.put("appKey", appKey);
        voiceMap.put("voiceUrl", voiceUrl);
        voiceMap.put("method", "voice.send.singlecall");
        voiceMap.put("accountCode", accountCode);
        voiceMap.put("billCode", "499979140021");
        String time = System.currentTimeMillis() + "";
        voiceMap.put("voiceCode", "Temp_2018007");
        voiceMap.put("appTimestamp", time);
        try {
            sendVoice(voiceMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 用于电话告警
     */
    private static void sendVoice(HashMap<String, String> parmMap) {
        if (!org.apache.commons.lang.StringUtils.isEmpty(parmMap.get("telephones"))) {
            VoiceInfo voiceInfo = new VoiceInfo();
            voiceInfo.setAppID(parmMap.get("appId"));
            voiceInfo.setAppVer(parmMap.get("appVer"));
            String appNotic = UUID.randomUUID().toString().replaceAll("-", "");
            voiceInfo.setAppNotic(appNotic);
            voiceInfo.setAppTimeStamp(parmMap.get("appTimestamp"));
            voiceInfo.setAppSign(md5AppAsign(parmMap.get("appId"), parmMap.get("appKey"), appNotic, parmMap.get("appTimestamp")));
            voiceInfo.setMethod(parmMap.get("method"));
            List<VoiceInfo.TData> tdatas = Lists.newArrayList();
            String[] var4 = (parmMap.get("telephones")).split(",");
            int var5 = var4.length;

            for (int var6 = 0; var6 < var5; ++var6) {
                String telephone = var4[var6];
                VoiceInfo.TData.TTplContent tTplContent = new VoiceInfo.TData.TTplContent();
                tTplContent.setContent(parmMap.get("message"));
                VoiceInfo.TData data = new VoiceInfo.TData();
                data.setAccountCode(parmMap.get("accountCode"));
                data.setBillCode(parmMap.get("billCode"));
                data.setMobile(telephone);
                data.setTplContent(tTplContent);
                data.setVoiceCode(parmMap.get("voiceCode"));
                tdatas.add(data);
            }

            voiceInfo.setData(tdatas);
            HttpClientUtils.doPostIgnore(parmMap.get("voiceUrl"), JSON.toJSONString(voiceInfo));
        }
    }

    /**
     * 生成MD5加密串
     */
    private static String md5AppAsign(String appId, String appKey, String appNotic, String timestamp) {
        StringBuilder mesBuf = new StringBuilder();
        String md5 = "";
        mesBuf.append(appId).append("@").append(appKey).append("@").append(appNotic).append("@").append(timestamp).append("@").append("ZTO.ApiGateway");
        try {
            md5 = EncryptUtils.md5Encrypt(mesBuf.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return md5;
    }
}
