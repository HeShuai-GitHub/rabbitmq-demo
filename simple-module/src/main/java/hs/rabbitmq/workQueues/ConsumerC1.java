package hs.rabbitmq.workQueues;

import com.rabbitmq.client.*;
import hs.rabbitmq.config.RabbitmqConfig;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbitmq-demo
 * @description: 消费者-接受消息
 * @create: 2021-01-04 21:47
 **/
public class ConsumerC1 {
    public static void main(String[] args) {
        try {
            final Channel channel = RabbitmqConfig.getChannel();
            /**
             * 这里解释一下为什么需要在订阅queue之前，提前queueDeclare一下，这个是为了防止provider还没有启动，而consumer先启动了，
             * 如果不提前声明的话，那么在rabbitmq中是不存在hello的，那么是没办法订阅消息的，反馈到程序中就是报错！
             * 但是声明时，也要特别注意参数不要弄错
             */
            channel.queueDeclare("work-queues",true,false,false,null);
            /**
             * 设置一次性接受信息的最大数量
             * 如果不设置，默认不受限制，那么Rabbitmq会一致发送消息至consumer这里，等待consumer处理
             * 这个其实是因为channel是异步处理消息的，哪怕下面设置了手动确认消息，消息依然会发送给consumer，等待处理确认
             * 设置了最大的接受消息数量后，在未确认消息之前，最多可以给consumer发送最大接受数量的消息，否则只能等待确认消息后才能继续发送
             */
            channel.basicQos(1);
            /**
             * 消费消息
             * queue:队列名称
             * autoAck：消息确认机制；true：自动确认消息，false：手动确认消息
             * callback：Consumer接口，收到消息后的处理逻辑！这里直接写了一个匿名类
             */
            channel.basicConsume("work-queues",false,new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag,
                                           Envelope envelope,
                                           AMQP.BasicProperties properties,
                                           byte[] body)
                        throws IOException
                {
                    try {
                        Thread.sleep(1000);
                        System.out.println("Receive==="+new String(body));
                        /**
                         * 手动确认消息
                         *  envelope.getDeliveryTag():delivery Tag,消息标识
                         *  multiple：批量处理消息
                         */
                        channel.basicAck(envelope.getDeliveryTag(),false);
                        /**
                         * 手动发送不确定消息
                         * envelope.getDeliveryTag():delivery Tag,消息标识
                         * multiple：批量处理消息
                         * requeue：重新排列，true:重新发送消息；false：丢弃或者死信队列
                         */
//                        channel.basicNack(envelope.getDeliveryTag(),false,true);
                        /**
                         * 拒绝消息
                         * envelope.getDeliveryTag():delivery Tag,消息标识
                         * requeue：重新排列，true:重新发送消息；false：丢弃或者死信队列
                         */
                        channel.basicReject(envelope.getDeliveryTag(),false);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

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