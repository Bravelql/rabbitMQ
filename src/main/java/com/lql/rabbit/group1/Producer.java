package com.lql.rabbit.group1;

/**
 * Created by LiuQiulan
 *
 * @date 2018-6-15 9:35
 *
 */

/**
 * 整个demo参考学习作者：朝向远方
 *
 * 学习地址：https://www.cnblogs.com/LipeiNet/category/896408.html
 */

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 消息生成者
 */
public class Producer {
    public final static String QUEUE_NAME="rabbitMQ.test";

    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        //设置RabbitMQ相关信息
        factory.setHost("localhost");
        /*factory.setUsername("admin");
        factory.setPassword("mypasswd");
        factory.setPort(5672);*/
        //创建一个新的连接
        Connection connection = factory.newConnection();
        //创建一个通道
        Channel channel = connection.createChannel();
        //  声明一个队列
        //channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String message = "Hello RabbitMQ";
        //发送消息到队列中
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
        System.out.println("Producer Send +'" + message + "'");
        //关闭通道和连接
        channel.close();
        connection.close();
    }
}

/**
 * channel.queueDeclare(QUEUE_NAME, false, false, false, null);
 * 注1：queueDeclare第一个参数表示队列名称、
 *      第二个参数为是否持久化（true表示是，队列将在服务器重启时生存）、
 *      第三个参数为是否是独占队列（创建者可以使用的私有队列，断开后自动删除）、
 *      第四个参数为当所有消费者客户端连接断开时是否自动删除队列、
 *      第五个参数为队列的其他参数
    channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
     注2：basicPublish
     第一个参数为交换机名称、
     第二个参数为队列映射的路由key、
     第三个参数为消息的其他属性、
     第四个参数为发送信息的主体
 */
