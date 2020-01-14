package com.zto.fire.demo;


import com.zto.fire.common.util.ValueUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

/**
 * 用于测试Java代码
 *
 * @author ChengLong 2019-9-4 13:39:36
 */
public class JavaTest implements MyBizable {
    @Override
    public String doSomething(String str) {
        return "服务端返回数据：" + str;
    }

    public static void main(String[] args) throws Exception {
        Server server = new RPC.Builder(new Configuration())
                .setProtocol(MyBizable.class)
                .setInstance(new JavaTest())
                .setBindAddress("10.1.54.130")
                .setPort(8077).build();
        server.start();
    }

}