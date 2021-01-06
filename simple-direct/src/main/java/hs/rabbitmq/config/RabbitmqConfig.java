package hs.rabbitmq.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbitmq-demo
 * @description: Rabbitmq工具类
 **/
public class RabbitmqConfig {

    /**
     * 创建连接mq的连接工厂对象
     */
    private static ConnectionFactory connectionFactory = new ConnectionFactory();
    static {
        // 连接rabbitmq的主机
        connectionFactory.setHost("59.110.41.57");
        // 设置连接端口号
        connectionFactory.setPort(5672);
        /**
         * 设置连接虚拟主机，虚拟主机：类似于nacos中命名空间概念
         * 举个例子：
         *      搭建一个Rabbitmq消息中间件后，此时有多组服务进行通信，原则上讲
         *      每一组服务之间是相互隔离的，也就是说，只允许A组内部服务之间进行相互通信，
         *      不允许A组和B组相互通信，这样就可以将它们划分在不同的虚拟主机中，完成这个功能
         */
        connectionFactory.setVirtualHost("/test");
        /**
         * 设置用户名
         */
        connectionFactory.setUsername("admin");
        // 设置密码
        connectionFactory.setPassword("admin");
    }

    /**
     * 获取连接对象
     * @return
     * @throws IOException
     * @throws TimeoutException
     */
    public static Connection getConnection() throws IOException, TimeoutException {
        return connectionFactory.newConnection();
    }

    /**
     * 获取连接中的通道
     * @return
     * @throws IOException
     * @throws TimeoutException
     */
    public static Channel getChannel() throws IOException, TimeoutException {
        return getConnection().createChannel();
    }
}
