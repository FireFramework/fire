package com.zto.bigdata.spark.common.util;

import com.jcraft.jsch.*;

import java.io.InputStream;

/**
 * ssh 工具类，可根据linux的用户名密码ssh到远程的linux服务器并执行命令
 *
 * @author ChengLong 2018年8月20日 08:05:33
 */
public class SSHUtils {
    // ssh session会话
    private Session session;

    /**
     * @param host
     * @param port
     * @param user
     * @param password
     * @throws JSchException
     */
    public SSHUtils(String host, Integer port, String user, String password) throws JSchException {
        connect(host, port, user, password);
    }

    /**
     * 连接sftp服务器
     *
     * @param host     远程主机ip地址
     * @param port     sftp连接端口，null 时为默认端口
     * @param user     用户名
     * @param password 密码
     * @return
     * @throws JSchException
     */
    private Session connect(String host, Integer port, String user, String password) throws JSchException {
        try {
            JSch jsch = new JSch();
            if (port != null) {
                session = jsch.getSession(user, host, port.intValue());
            } else {
                session = jsch.getSession(user, host);
            }
            session.setPassword(password);
            // 设置第一次登陆的时候提示，可选值:(ask | yes | no)
            session.setConfig("StrictHostKeyChecking", "no");
            // 30秒连接超时
            session.connect(10000);
        } catch (JSchException e) {
            e.printStackTrace();
            System.out.println("SFTPUitl 获取连接发生错误");
            throw e;
        }
        return session;
    }

    /**
     * 执行命令，返回执行结果
     * @param command 命令
     * @return String 执行命令后的返回
     * @throws JSchException
     */
    public SSHResInfo sendCmd(String command) throws Exception {
        return sendCmd(command, 200);
    }

    /**
     * 执行命令，返回执行结果
     * @param command 命令
     * @param delay 估计shell命令执行时间
     * @return String 执行命令后的返回
     * @throws JSchException
     */
    public SSHResInfo sendCmd(String command, int delay) throws Exception {
        if (delay < 50) {
            delay = 50;
        }
        SSHResInfo result = null;
        byte[] tmp = new byte[1024]; // 读数据缓存
        StringBuffer strBuffer = new StringBuffer(); // 执行SSH返回的结果
        StringBuffer errResult = new StringBuffer();

        Channel channel = session.openChannel("exec");
        ChannelExec ssh = (ChannelExec) channel;
        // 返回的结果可能是标准信息,也可能是错误信息,所以两种输出都要获取
        // 一般情况下只会有一种输出.
        // 但并不是说错误信息就是执行命令出错的信息,如获得远程java JDK版本就以
        // ErrStream来获得.
        InputStream stdStream = ssh.getInputStream();
        InputStream errStream = ssh.getErrStream();

        ssh.setCommand(command);
        ssh.connect();

        try {
            // 开始获得SSH命令的结果
            while (true) {
                // 获得错误输出
                while (errStream.available() > 0) {
                    int i = errStream.read(tmp, 0, 1024);
                    if (i < 0)
                        break;
                    errResult.append(new String(tmp, 0, i));
                }

                // 获得标准输出
                while (stdStream.available() > 0) {
                    int i = stdStream.read(tmp, 0, 1024);
                    if (i < 0)
                        break;
                    strBuffer.append(new String(tmp, 0, i));
                }
                if (ssh.isClosed()) {
                    int code = ssh.getExitStatus();
                    result = new SSHResInfo(code, strBuffer.toString(), errResult.toString());
                    break;
                }
                try {
                    Thread.sleep(delay);
                } catch (Exception ee) {
                    ee.printStackTrace();
                }
            }
        } finally {
            // TODO: handle finally clause
            channel.disconnect();
        }
        return result;
    }


    /**
     * 用完记得关闭，否则连接一直存在，程序不会退出
     */
    public void close() {
        if (session.isConnected()) {
            session.disconnect();
        }
    }

    /**
     * ssh返回信息包装类
     */
    public class SSHResInfo {
        private int exitStuts;//返回状态码 （在linux中可以通过 echo $? 可知每步执行令执行的状态码）
        private String outRes;//标准正确输出流内容
        private String errRes;//标准错误输出流内容

        public SSHResInfo(int exitStuts, String outRes, String errRes) {
            super();
            this.exitStuts = exitStuts;
            this.outRes = outRes;
            this.errRes = errRes;
        }

        public SSHResInfo() {
            super();
        }

        public int getExitStuts() {
            return exitStuts;
        }

        public void setExitStuts(int exitStuts) {
            this.exitStuts = exitStuts;
        }

        public String getOutRes() {
            return outRes;
        }

        public void setOutRes(String outRes) {
            this.outRes = outRes;
        }

        public String getErrRes() {
            return errRes;
        }

        public void setErrRes(String errRes) {
            this.errRes = errRes;
        }

        /**
         * 当exitStuts=0 && errRes="" &&outREs=""返回true
         *
         * @return
         */
        public boolean isEmptySuccess() {
            if (this.getExitStuts() == 0 && "".equals(this.getErrRes()) && "".equals(this.getOutRes())) {
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return "SSHResInfo [exitStuts=" + exitStuts + ", outRes=" + outRes + ", errRes=" + errRes + "]";
        }

        public void clear() {
            exitStuts = 0;
            outRes = errRes = null;
        }
    }

    public static void main(String args[]) {
        try {
            // 使用目标服务器机上的用户名和密码登陆
            SSHUtils helper = new SSHUtils("172.18.18.117", 23245, "root", "Zy(2$6z[3@2i#l(");
            String command = "ls";
            try {
                SSHResInfo resInfo = helper.sendCmd(command);
                System.out.println(resInfo.toString());
                helper.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (JSchException e) {
            e.printStackTrace();
        }

    }

}
