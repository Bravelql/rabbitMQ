package com.lql.rabbit.group5;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by LiuQiulan
 *
 * @date 2018-6-15 17:03
 *
 * 参考学习：http://www.cnblogs.com/LipeiNet/p/5980802.html
 * 远程调用：在远程计算机上运行一个函数，并等待结果
 *
 * 使用RabbitMQ搭建一个RPC系统，一个客户端和一个可扩展的RPC服务器
 */
public class RPCServer {
    private static final String RPC_QUEUE_NAME = "rpc_queue";

    private static int fib(int n) {
        if (n == 0) {
            return 0;
        }
        if (n == 1) {
            return 1;
        }
        return fib(n - 1) + fib(n - 1);
    }

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        //建立连接，通道，队列
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
        //我们可能运行多个服务器进程，为了分散负载服务器压力，我们设置channel.basicQos(1);
        // 保证一次只分发一个,可以是服务端发送，也可以是客户端接收
        channel.basicQos(1);

        //我们用basicconsume访问队列。然后进入循环，在其中我们等待请求消息并处理消息然后发送响应。
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(RPC_QUEUE_NAME, false, consumer);

        System.out.println("RPCServer Awating RPC request");
        while (true) {
            //rpc服务器是等待队列的请求，当收到一个请求
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            BasicProperties props = delivery.getProperties();
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder().
                    correlationId(props.getCorrelationId()).build();

            String message = new String(delivery.getBody(), "UTF-8");
            int n = Integer.parseInt(message);

            System.out.println("RPCServer fib(" + message + ")");
            String response = "" + fib(n);
            //消息返回的结果返回给客户端，使请求结束。
            channel.basicPublish( "", props.getReplyTo(), replyProps, response.getBytes());
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
    }
}
