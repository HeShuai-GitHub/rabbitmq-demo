package hs.rabbitmq.exchange.header;

import com.rabbitmq.client.*;
import hs.rabbitmq.config.RabbitmqConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbitmq-demo
 * @description: 消费者-接受消息, Header all
 **/
public class Consumer_1 {

    private final static String EXCHANGE = "MESSAGE_HEADER";

    public static void main(String[] args) {
        try {
            Channel channel = RabbitmqConfig.getChannel();
            // 声明交换机
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.HEADERS);
            // 声明一个独占、自动删除的queue
            String queueNameAll = channel.queueDeclare().getQueue();
            /**
             * 绑定参数
             * x-match：
             *       all： 默认值。一个传送消息的header里的键值对和交换机的绑定参数键值对全部匹配，才可以路由到对应交换机
             *       any: 一个传送消息的header里的键值对和交换机的绑定参数键值对任意一个匹配，就可以路由到对应交换机
             *       注：不管两种哪个模式，不需要传送消息Header属性和交换机绑定参数个数完全一致，只需要满足交换机绑定参数即可，也就是说可以传送消息Header属性可以多于交换机绑定参数个数，只需要
             *       传送消息Header属性其中某些属性符合交换机绑定参数的规则就可以了
             */
            Map<String,Object> headerAll = new HashMap<>();
            headerAll.put("x-match","all");
            headerAll.put("flag","consumer");
            headerAll.put("tag","test");
            /**
             * queue: 队列名字
             * exchange：交换机名字
             * routingKey：路由key，当Exchange type是 Header时，可以默认“”
             * arguments：定义绑定参数，目前只有Exchange type是Header时使用到
             */
            channel.queueBind(queueNameAll,EXCHANGE,"",headerAll);

            /**
             * 消费消息
             * queue:队列名称
             * autoAck：消息确认机制；true：自动确认消息，false：手动确认消息
             * callback：Consumer接口，收到消息后的处理逻辑！这里直接写了一个匿名类
             */
            channel.basicConsume(queueNameAll,false,new DefaultConsumer(channel){
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
