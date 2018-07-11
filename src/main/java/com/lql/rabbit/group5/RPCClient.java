package com.lql.rabbit.group5;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
/**
 * Created by LiuQiulan
 *
 * @date 2018-6-20 11:05
 */
public class RPCClient {
    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";
    private String replyQueueName;
    private QueueingConsumer consumer;

    //当客户端启动时，它创建一个匿名的独占回调队列。
    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        replyQueueName = channel.queueDeclare().getQueue();//匿名队列
        consumer = new QueueingConsumer(channel);
        channel.basicConsume(replyQueueName, true, consumer);
    }

    public String call(String message) throws IOException, InterruptedException {
        String response;
        String corrID = UUID.randomUUID().toString();
        //对于rpc请求，客户端发送2个属性，一个是replyTo设置回调队列，另一是correlationId为每个队列设置唯一值
        AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                .correlationId(corrID).replyTo(replyQueueName).build();
        //请求被发送到一个rpc_queue队列中
        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));
        while (true) {
            //服务器返回结果
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            //检查correlationId，如果它和从请求返回的值匹配，就进行响应
            if (delivery.getProperties().getCorrelationId().equals(corrID)) {
                response = new String(delivery.getBody(), "UTF-8");
                break;
            }
        }
        return response;
    }

    public void close() throws Exception {
        connection.close();
    }

    public static void main(String[] args) throws Exception {
        RPCClient rpcClient = null;
        String response;
        try {
            rpcClient = new RPCClient();
            System.out.println("RPCClient  Requesting fib(20)");
            response = rpcClient.call("20");
            System.out.println("RPCClient  Got '" + response + "'");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rpcClient != null) {
                rpcClient.close();
            }
        }
    }
}