package com.lql.rabbit.group3;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by LiuQiulan
 *
 * @date 2018-6-15 15:49
 */

/**
 * 采用路由的方式对不同的消息进行过滤
 */
public class RoutingSendDirect {
    private static final String EXCHANGE_NAME = "direct_logs";
    // 路由关键字
    private static final String[] routingKeys = new String[]{"info" ,"warning", "error"};
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        //声明交换机
        channel.exchangeDeclare(EXCHANGE_NAME,"direct");//注意是direct
        //发送信息
        for (String routingKey:routingKeys){
            String message = "RoutingSendDirect Send the message level:" + routingKey;
            channel.basicPublish(EXCHANGE_NAME,routingKey,null,message.getBytes());
            System.out.println("RoutingSendDirect Send"+routingKey +"':'" + message);
        }
        channel.close();
        connection.close();
    }
}
