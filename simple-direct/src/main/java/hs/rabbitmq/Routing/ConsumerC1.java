package hs.rabbitmq.Routing;

import com.rabbitmq.client.*;
import hs.rabbitmq.config.RabbitmqConfig;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbitmq-demo
 * @description: 消费者-接受消息
 * @create: 2021-01-05 22:55
 **/
public class ConsumerC1 {
    private final static String EXCHANGE = "MESSAGE_DIRECT";

    public static void main(String[] args) {
        try {
            final Channel channel = RabbitmqConfig.getChannel();
            /**
             * 声明一个Exchange
             * exchange：名称
             * type：类型
             */
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT);
            /**
             * 随机声明一个队列，并获取对应的queue的名称
             * 默认特性：
             * autoDelete:true
             * exclusive:true
             */
            String queueName = channel.queueDeclare().getQueue();
            /**
             * 绑定queue到exchange
             * queue：队列名称
             * exchange：交换机名称
             * routingKey：DIRECT类型，必须需要routingKey
             */
            channel.queueBind(queueName,EXCHANGE,"error");
            channel.queueBind(queueName,EXCHANGE,"info");
            channel.queueBind(queueName,EXCHANGE,"warm");
            /**
             * 消费消息
             * queue:队列名称
             * autoAck：消息确认机制；true：自动确认消息，false：手动确认消息
             * callback：Consumer接口，收到消息后的处理逻辑！这里直接写了一个匿名类
             */
            channel.basicConsume(queueName,true,new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag,
                                           Envelope envelope,
                                           AMQP.BasicProperties properties,
                                           byte[] body)
                        throws IOException
                {
                    System.out.println("Receive==="+new String(body));

                }
            });
            // 若不关闭connection，则一直保持接受消息的状态
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}
