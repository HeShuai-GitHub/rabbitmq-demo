package hs.rabbitmq.exchange.header;

import com.rabbitmq.client.*;
import hs.rabbitmq.config.RabbitmqConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbitmq-demo
 * @description: 消费者-接受消息  Header any
 **/
public class Consumer_2 {

    private final static String EXCHANGE = "MESSAGE_HEADER";

    public static void main(String[] args) {
        try {
            Channel channel = RabbitmqConfig.getChannel();
            // 声明交换机
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.HEADERS);

            String queueNameAny = channel.queueDeclare().getQueue();
            Map<String,Object> headerAny = new HashMap<>();
            headerAny.put("x-match","any");
            headerAny.put("flag","consumer");
            channel.queueBind(queueNameAny,EXCHANGE,"",headerAny);

            /**
             * 消费消息
             * queue:队列名称
             * autoAck：消息确认机制；true：自动确认消息，false：手动确认消息
             * callback：Consumer接口，收到消息后的处理逻辑！这里直接写了一个匿名类
             */
            channel.basicConsume(queueNameAny,false,new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag,
                                           Envelope envelope,
                                           AMQP.BasicProperties properties,
                                           byte[] body)
                        throws IOException
                {
                    System.out.println("Receive==="+new String(body));
                    channel.basicAck(envelope.getDeliveryTag(),true);
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
