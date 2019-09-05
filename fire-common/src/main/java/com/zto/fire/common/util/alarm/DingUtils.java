package com.zto.fire.common.util.alarm;

import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiGettokenRequest;
import com.dingtalk.api.request.OapiMessageCorpconversationAsyncsendV2Request;
import com.dingtalk.api.response.OapiGettokenResponse;

/**
 * 钉钉告警
 *
 * @author ChengLong 2019-9-4 16:28:53
 */
public class DingUtils {
    private final static String token = "https://oapi.dingtalk.com/gettoken";
    private final static String corpid = "ding48c6861d5cb6fb4535c2f4657eb6378f";
    private final static String corpsecret = "4HR3plYZ_esOw23tk2ueLMvUybRpmITWagamVq68UDhFnPIzfq74_bmotPYCy6PI";

    /**
     * 钉钉消息推送
     *
     * @param content 消息内容
     */
    public static void sendMsg(String dingdingId, String content) {
        try {
            DingTalkClient client = new DefaultDingTalkClient(token);
            OapiGettokenRequest request = new OapiGettokenRequest();
            request.setCorpid(corpid);
            request.setCorpsecret(corpsecret);
            request.setHttpMethod("GET");
            OapiGettokenResponse response = client.execute(request);
            String accessToken = response.getAccessToken();
            DingTalkClient client1 = new DefaultDingTalkClient("https://oapi.dingtalk.com/topapi/message/corpconversation/asyncsend_v2");
            OapiMessageCorpconversationAsyncsendV2Request request1 = new OapiMessageCorpconversationAsyncsendV2Request();
            request1.setUseridList(dingdingId);
            request1.setUseridList(request1.getUseridList() + ",104530081123132671");
            request1.setAgentId(187985521L);
            request1.setToAllUser(false);
            OapiMessageCorpconversationAsyncsendV2Request.Msg msg = new OapiMessageCorpconversationAsyncsendV2Request.Msg();
            msg.setMsgtype("text");
            msg.setText(new OapiMessageCorpconversationAsyncsendV2Request.Text());
            msg.getText().setContent(content);
            request1.setMsg(msg);
            client1.execute(request1, accessToken);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
