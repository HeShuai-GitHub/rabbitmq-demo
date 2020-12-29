package hs.rabbitmq.simple;

import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.ChannelN;
import hs.rabbitmq.simple.config.RabbitmqConfig;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbitmq-demo
 * @description: 消息生产者
 **/
public class Provider {

    @Test
    public void publishing() {
        try {
            // 通过工具类获取Channel
            Channel channel = RabbitmqConfig.getChannel();
            /**
             *  声明队列
             *    queue: 队列名称 ,若队列已存在，但是参数不一致，则报错！
             *    durable：队列持久化，若为true，则rabbitmq重启后仍然存在
             *    exclusive：排它性，true：只可以此Connection连接这个queue，当当前Connection关闭时，这个queue会自动删除！
             *              并且在存在过程中，其他的Connection不可以连接这个queue，原理嘛，就是加个排它锁
             *    autoDelete：自动删除，true：当queue不再使用后自动删除；不再使用：即queue中无数据，没有其它connection连接此queue
             *    arguments：额外参数
             */
            channel.queueDeclare("hello",false,false,false,null);
            /**
             * 发布消息
             *  exchange: exchange名称
             *  routingKey:路由名称，不一定是某个对应的queue名称，当然肯定是需要匹配到某个queue的
             *  props：额外配置
             *  body：消息体
             */
            channel.basicPublish("","hello",null,"hello rabbitmq".getBytes());
            // 关闭channel
            channel.close();
            // 关闭Connection
            channel.getConnection().close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 测试队列持久化及消息持久化
     */
    @Test
    public void testDurable() throws IOException, TimeoutException {
        Channel channel = RabbitmqConfig.getChannel();
        // 声明队列持久化
        channel.queueDeclare("durableQueue",true,false,false,null);
        // 声明消息持久化
        channel.basicPublish("","durableQueue", MessageProperties.PERSISTENT_TEXT_PLAIN,"持久化消息".getBytes());
    }
    /**
     * 测试消息过期时间
     */
    @Test
    public void testTTL() throws IOException, TimeoutException {
        Map<String, Object> arguments = new HashMap<String, Object>();
        // 队列内消息十秒过期
        arguments.put("x-message-ttl", 10000);
        // 队列十秒没有消费者访问该队列则自动删除
        arguments.put("x-expires", 20000);
        Channel channel = RabbitmqConfig.getChannel();
        // 声明队列内消息的过期时间
        channel.queueDeclare("ttlQueue",false,false,false,arguments);
        channel.basicPublish("","ttlQueue",null,"10秒后消息过期".getBytes());
        // 设置单个消息过期时间
        AMQP.BasicProperties.Builder properties = new AMQP.BasicProperties.Builder().expiration(20000+"");
        channel.basicPublish("","durableQueue",properties.build(),"20秒后消息过期".getBytes());
    }

    /**
     * x-max-length:用于指定队列的长度，如果不指定，可以认为是无限长，例如指定队列的长度是4，当超过4条消息，前面的消息将被删除，给后面的消息腾位，类似于栈的结构，
     * 当设置了x-max-priority后，优先级高的排在前面，所以基本上排除的话就是排除优先级高的这些
     * x-max-length-bytes: 用于指定队列存储消息的占用空间大小，当达到最大值是会删除之前的数据腾出空间
     * x-max-priority: 设置消息的优先级，优先级值越大，越被提前消费。
     */
    @Test
    public void testMax() throws IOException, TimeoutException {
        Map<String, Object> arguments = new HashMap<String, Object>();
        arguments.put("x-max-length", 4);
        arguments.put("x-max-length-bytes", 1024);
        arguments.put("x-max-priority", 5);
        Channel channel = RabbitmqConfig.getChannel();
        declareDead(arguments,channel);
        channel.queueDeclare("maxQueue",false,false,false,arguments);
        for (int i=1; i<=6;++i){
            AMQP.BasicProperties.Builder properties = new AMQP.BasicProperties.Builder().priority(i);
            channel.basicPublish("","maxQueue",properties.build(),("第"+i+"条消息，优先级是："+i).getBytes());
        }
    }

    /**
     * 声明Dead-exchange、dead-queue
     */
    public void declareDead(Map<String, Object> arguments,Channel channel) throws IOException {
        channel.exchangeDeclare("EXCHANGE_DEAD",BuiltinExchangeType.DIRECT);
        channel.queueDeclare("QUEUE_DEAD",false,false,false,null);
        // 若不指定exchange、queue,则默认使用AMQP default默认exchange，默认使用queue的名字作为routingkey
        channel.queueBind("QUEUE_DEAD","EXCHANGE_DEAD","routing_dead");
        arguments.put("x-dead-letter-exchange", "EXCHANGE_DEAD");
        arguments.put("x-dead-letter-routing-key", "routing_dead");
    }

    /**
     * 测试排它性exclusive
     */
    public static void main(String[] args) {
        Connection connection = null;
        try {
            connection = RabbitmqConfig.getConnection();
            Channel channel = connection.createChannel();
            // 声明一个排它queue
            channel.queueDeclare("testExclusive",false,true,false,null);
            // 关闭当前channel，以证明queue的排它性和channel没关系
            channel.close();
            testExclusiveA(connection);
            testExclusiveB(connection);
            // 在一个服务项目中是没有办法测试排它性的，因为本项目中所有对rabbitmq的连接，在Rabbitmq看来都是一个Connection，所以是不会触发排它锁的，如果需要测试，可以再创建一个项目进行测试
            testExclusiveC();
            // 等待十秒，这区间可以看一下Rabbitmq ui界面，此时rabbitmq ui是可以看到此queue的
            Thread.sleep(10000);
            // 关闭连接，关闭连接后，testExclusive这个queue也会随之删除
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试发送消息
     * @param connection
     * @throws IOException
     * @throws TimeoutException
     */
    public static void testExclusiveA(Connection connection) throws IOException, TimeoutException {
        Channel channel = connection.createChannel();
        channel.basicPublish("","testExclusive",null,"hello testExclusiveA".getBytes());
        System.out.println("testExclusiveA");
        channel.close();
    }

    /**
     * 测试发送消息
     * @param connection
     * @throws IOException
     * @throws TimeoutException
     */
    public static void testExclusiveB(Connection connection) throws IOException, TimeoutException {
        Channel channel = connection.createChannel();
        channel.basicPublish("","testExclusive",null,"hello testExclusiveB".getBytes());
        System.out.println("testExclusiveB");
        channel.close();
    }

    /**
     * 这里重新建了一个ConnectionFactory，并且获取一个新的Connection，但是事实证明，也是可以发送消息到testExclusive的，因为在同一个项目中建立的连接，
     * 在Rabbitmq看来是一个Connection
     * @throws IOException
     * @throws TimeoutException
     */
    public static void testExclusiveC() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("59.110.41.57");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/test");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.basicPublish("","testExclusive",null,"hello testExclusiveC".getBytes());
        System.out.println("testExclusiveC");
        channel.close();
        connection.close();
    }
}
