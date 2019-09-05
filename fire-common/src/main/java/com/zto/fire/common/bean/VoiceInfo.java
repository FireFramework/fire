package com.zto.fire.common.bean;

import java.util.List;

/**
 * 语音告警封装类
 * @author ChengLong 2019-9-4 17:07:56
 */
public class VoiceInfo {
    private List<TData> data;
    private String appTimeStamp;
    private String appNotic;
    private String appVer;
    private String appSign;
    private String method;
    private String appID;

    public VoiceInfo() {
    }

    public void setData(List<TData> value) {
        this.data = value;
    }

    public List<TData> getData() {
        return this.data;
    }

    public void setAppTimeStamp(String value) {
        this.appTimeStamp = value;
    }

    public String getAppTimeStamp() {
        return this.appTimeStamp;
    }

    public void setAppNotic(String value) {
        this.appNotic = value;
    }

    public String getAppNotic() {
        return this.appNotic;
    }

    public void setAppVer(String value) {
        this.appVer = value;
    }

    public String getAppVer() {
        return this.appVer;
    }

    public void setAppSign(String value) {
        this.appSign = value;
    }

    public String getAppSign() {
        return this.appSign;
    }

    public void setMethod(String value) {
        this.method = value;
    }

    public String getMethod() {
        return this.method;
    }

    public void setAppID(String value) {
        this.appID = value;
    }

    public String getAppID() {
        return this.appID;
    }

    public static class TData {
        private String voiceCode;
        private String billCode;
        private String mobile;
        private String accountCode;
        private TTplContent tplContent;

        public TData() {
        }

        public void setVoiceCode(String value) {
            this.voiceCode = value;
        }

        public String getVoiceCode() {
            return this.voiceCode;
        }

        public void setBillCode(String value) {
            this.billCode = value;
        }

        public String getBillCode() {
            return this.billCode;
        }

        public void setMobile(String value) {
            this.mobile = value;
        }

        public String getMobile() {
            return this.mobile;
        }

        public void setAccountCode(String value) {
            this.accountCode = value;
        }

        public String getAccountCode() {
            return this.accountCode;
        }

        public void setTplContent(TTplContent value) {
            this.tplContent = value;
        }

        public TTplContent getTplContent() {
            return this.tplContent;
        }

        public static class TTplContent {
            private String content;

            public TTplContent() {
            }

            public void setContent(String value) {
                this.content = value;
            }

            public String getContent() {
                return this.content;
            }
        }
    }
}
