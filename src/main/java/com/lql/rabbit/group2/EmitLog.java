package com.lql.rabbit.group2;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by LiuQiulan
 *
 * @date 2018-6-15 15:20
 */

/**
 * 信息发送端代码
 *
 * 在上一篇说到的队列都指定了名称，但是现在我们不需要这么做，我们需要所有的日志信息，而不只是其中的一个。
 * 如果要做这样的队列，我们需要2件事，一个就是获取一个新的空的队列，这样我就需要创建一个随机名称的队列，最好让服务器帮我们做出选择，
 * 第二个就是我们断开用户的队列，应该自动进行删除
 *
 * 采用广播的模式进行消息的发送
 *
 * 完成一个发布/订阅模式的消息队列
 */
public class EmitLog {
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");//fanout表示分发，所有的消费者得到同样的队列信息
        //分发信息
        for (int i = 0; i < 5; i++) {
            String message = "Hello World" + i;
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
            System.out.println("EmitLog Sent '" + message + "'");
            doWork(message);//可以看到生产者每发布一条消息，消费者就会接收到一条下次
        }
        channel.close();
        connection.close();
    }

    private static void doWork(String task) {
        try {
            Thread.sleep(2000); // 暂停1秒钟
        } catch (InterruptedException _ignored) {
            Thread.currentThread().interrupt();
        }
    }
}