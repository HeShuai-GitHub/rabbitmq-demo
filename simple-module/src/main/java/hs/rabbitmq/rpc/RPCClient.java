package hs.rabbitmq.rpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import hs.rabbitmq.config.RabbitmqConfig;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * @title: RPCClient
 * @description: 关于RPC的解释应该在博客中解释的很清楚了，这里就不赘述了
 *                下面主要是使用基于Rabbitmq实现RPC请求的过程，这个类是Client，发送RPC请求并且得到RPC的一个请求结果
 * @author heshuai
 * @date 2021年01月19日 21:11
 */
public class RPCClient {

    private static Channel channel;
    private static String requestQueueName = "rpc_queue";

    public static void main(String[] argv) throws IOException {
        try {
            // 新建一个channel
            channel = RabbitmqConfig.getChannel();
            // 循环调用RPCServer，查看返回结果
            for (int i = 0; i < 16; i++) {
                String i_str = Integer.toString(i);
                System.out.println(" [x] Requesting fib(" + i_str + ")");
                String response = call(i_str);
                System.out.println(" [.] Got '" + response + "'");
            }
        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }finally {
            // 关闭connection
            channel.getConnection().close();
        }
    }

    /**
     * RPC请求实际执行方法
     * @param message
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static String call(String message) throws IOException, InterruptedException {
        // 生成唯一的correlationId,用来辨别返回response
        final String corrId = UUID.randomUUID().toString();
        // 生成返回queue，也就是当RPC request到达server后，由server返回的response所发送的queue
        String replyQueueName = channel.queueDeclare().getQueue();
        /**
         * 定义每条消息的配置信息，主要配置其中的replyTo，correlationId
         * replyTo：response返回queue
         * correlationId：辨别queue中的response是否符合当前的request
         */
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();
        // 发送request请求
        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));
        // 创建一个阻塞queue，数量为1
        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);
        /**
         * 监听response返回queue
         * queue: queue名字
         * autoAck：是否自动确认消息
         * deliverCallback：发送消息时的回调
         * cancelCallback：取消回调通知
         * return: 返回consumerTag，唯一标识consumer在queue中的状态
         */
        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            // 接受等于当前correlationId的Response，若不等于则丢弃
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                // 插入一个元素
                response.offer(new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {
        });
        // 获取一个元素，若当前无元素，则block当前线程，直到可以或许元素为止；类似于Future方法
        String result = response.take();
        // 取消回调通知
        channel.basicCancel(ctag);
        return result;
    }

}
