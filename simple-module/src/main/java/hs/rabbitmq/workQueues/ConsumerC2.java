package hs.rabbitmq.workQueues;

import com.rabbitmq.client.*;
import hs.rabbitmq.config.RabbitmqConfig;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbitmq-demo
 * @description: 消费者-接受消息
 * @create: 2021-01-04 21:48
 **/
public class ConsumerC2 {
    public static void main(String[] args) {
        try {
            Channel channel = RabbitmqConfig.getChannel();
            /**
             * 这里解释一下为什么需要在订阅queue之前，提前queueDeclare一下，这个是为了防止provider还没有启动，而consumer先启动了，
             * 如果不提前声明的话，那么在rabbitmq中是不存在hello的，那么是没办法订阅消息的，反馈到程序中就是报错！
             * 但是声明时，也要特别注意参数不要弄错
             */
            channel.queueDeclare("work-queues",true,false,false,null);
            /**
             * 消费消息
             * queue:队列名称
             * autoAck：消息确认机制；true：自动确认消息，false：手动确认消息
             * callback：Consumer接口，收到消息后的处理逻辑！这里直接写了一个匿名类
             */
            channel.basicConsume("work-queues",true,new DefaultConsumer(channel){
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