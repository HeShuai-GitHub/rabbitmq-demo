package hs.rabbitmq.rpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import hs.rabbitmq.config.RabbitmqConfig;

/**
 * @author heshuai
 * @title: RPCServer
 * @description:  RPC服务端，基于Rabbitmq（AMQP）实现RPC系统，这里接受到一个数字参数并返回当前数字所对应Fibonacci序列位置的值
 * @date 2021年01月19日 22:35
 */
public class RPCServer {
    // 监听queue名字
    private static final String RPC_QUEUE_NAME = "rpc_queue";
    private static Channel channel;

    /**
     * 返回一个Fibonacci数字
     * @param n 处于Fibonacci序列中的位置
     * @return 返回Fibonacci数值
     */
    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] argv) throws Exception {

        try {
            channel = RabbitmqConfig.getChannel();
            /**
             * 声明queue，其他参数就不介绍了，其他模型中介绍太多次了
             */
            channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
            /**
             * 清除指定queue的内容，就是将queue中消息清空
             */
            channel.queuePurge(RPC_QUEUE_NAME);
            /**
             * 设置一次性预处理消息数量
             */
            channel.basicQos(1);

            System.out.println(" [x] Awaiting RPC requests");

            Object monitor = new Object();

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                /**
                 * 响应Message的配置信息
                 * correlationId：与Request相关联的唯一Id
                  */
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(delivery.getProperties().getCorrelationId())
                        .build();
                /**
                 * RPC响应结果
                 */
                String response = "";

                try {
                    // Request参数
                    String message = new String(delivery.getBody(), "UTF-8");
                    int n = Integer.parseInt(message);

                    System.out.println(" [.] fib(" + message + ")");
                    // 获取Fibonacci数字，并封装结果
                    response += fib(n);
                } catch (RuntimeException e) {
                    System.out.println(" [.] " + e.toString());
                } finally {
                    // 返回Response给Client，采用默认Exchange
                    channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                    // 手动确认单条消息
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    // RabbitMq consumer worker thread notifies the RPC server owner thread
                    synchronized (monitor) {
                        monitor.notify();
                    }
                }
            };

            channel.basicConsume(RPC_QUEUE_NAME, false, deliverCallback, (consumerTag -> { }));
            // 使main线程不关闭，等待consume消息
            while (true) {
                synchronized (monitor) {
                    try {
                        monitor.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            // 关闭连接
            channel.getConnection().close();
        }
    }
}
