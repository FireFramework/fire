package com.zto.fire.common.util.alarm;

import com.zto.fire.common.bean.MailInfo;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

/**
 * 邮件工具类
 *
 * @author ChengLong 2019-9-5 14:05:26
 */
public class MailUtils {

    /**
     * 以文本格式发送邮件
     *
     * @param mailInfo 待发送的邮件的信息
     */
    public static boolean sendTextMail(MailInfo mailInfo) {
        // 判断是否需要身份认证
        Properties pro = mailInfo.getProperties();
        // 根据邮件会话属性和密码验证器构造一个发送邮件的session
        Session sendMailSession = Session.getDefaultInstance(pro);
        try {
            // 根据session创建一个邮件消息
            Message mailMessage = new MimeMessage(sendMailSession);
            // 创建邮件发送者地址
            Address from = new InternetAddress(mailInfo.getFromAddress());
            // 设置邮件消息的发送者
            mailMessage.setFrom(from);
            // 创建邮件的接收者地址，并设置到邮件消息中
            InternetAddress[] sendTo = new InternetAddress[mailInfo.getToAddress().split(";").length];
            for (int i = 0; i < mailInfo.getToAddress().split(";").length; i++) {
                sendTo[i] = new InternetAddress(mailInfo.getToAddress().split(";")[i]);
            }

            mailMessage.setRecipients(javax.mail.internet.MimeMessage.RecipientType.TO, sendTo);
            // 设置邮件消息的主题
            mailMessage.setSubject(mailInfo.getSubject());
            // 设置邮件消息发送的时间
            mailMessage.setSentDate(new Date());
            // 设置邮件消息的主要内容
            String mailContent = mailInfo.getContent();
            mailMessage.setText(mailContent);
            // 发送邮件
            Transport.send(mailMessage);
            return true;
        } catch (MessagingException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    /**
     * 以HTML格式发送邮件
     *
     * @param mailInfo 待发送的邮件信息
     */
    public static boolean sendHtmlMail(String userName, String password, MailInfo mailInfo) {
        // 判断是否需要身份认证
        Properties pro = mailInfo.getProperties();
        // 如果需要身份认证，则创建一个密码验证器
        // 根据邮件会话属性和密码验证器构造一个发送邮件的session
        Session sendMailSession = Session.getInstance(pro, new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(userName, password);
            }
        });
        try {
            Transport transport = sendMailSession.getTransport("smtp");
            transport.connect(userName, password);
            // 根据session创建一个邮件消息
            Message mailMessage = new MimeMessage(sendMailSession);
            // 创建邮件发送者地址
            Address from = new InternetAddress(mailInfo.getFromAddress());
            // 设置邮件消息的发送者
            mailMessage.setFrom(from);
            // 创建邮件的接收者地址，并设置到邮件消息中
            InternetAddress[] sendTo = new InternetAddress[mailInfo.getToAddress().split(";").length];
            for (int i = 0; i < mailInfo.getToAddress().split(";").length; i++) {
                sendTo[i] = new InternetAddress(mailInfo.getToAddress().split(";")[i]);
            }
            mailMessage.setRecipients(javax.mail.internet.MimeMessage.RecipientType.TO, sendTo);
            // 设置邮件消息的主题
            mailMessage.setSubject(mailInfo.getSubject());
            // 设置邮件消息发送的时间
            mailMessage.setSentDate(new Date());
            // MiniMultipart类是一个容器类，包含MimeBodyPart类型的对象
            Multipart mainPart = new MimeMultipart();
            // 创建一个包含HTML内容的MimeBodyPart
            BodyPart html = new MimeBodyPart();
            // 设置HTML内容
            html.setContent(mailInfo.getContent(), "text/html; charset=utf-8");
            mainPart.addBodyPart(html);
            // 将MiniMultipart对象设置为邮件内容
            mailMessage.setContent(mainPart);
            // 发送邮件
            transport.send(mailMessage);
            return true;
        } catch (MessagingException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    /**
     * 邮件发送(HTML格式发送邮件)
     */
    public static void sendHtmlMail(HashMap<String, String> mailMap) {
        MailInfo mailInfo = new MailInfo(mailMap.get("mailServerHost"), mailMap.get("mailServerPort"), mailMap.get("mailFromAddress"), mailMap.get("mailToAddress"), mailMap.get("userName"), mailMap.get("password"), mailMap.get("subject"), mailMap.get("content") + "<br>");
        if (mailMap.get("mailToAddress") != null) {
            mailInfo.addToAddress(mailMap.get("mailToAddress"));
        }
        MailUtils.sendHtmlMail(mailMap.get("userName"), mailMap.get("password"), mailInfo);
    }
}
