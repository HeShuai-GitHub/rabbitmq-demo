package hs.rabbitmq.publishSubscribe;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import hs.rabbitmq.config.RabbitmqConfig;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbitmq-demo
 * @description: 消息生产者
 * @create: 2021-01-05 22:23
 **/
public class Producer {

    private final String EXCHANGE = "MESSAGE_FANOUT";
    private final String MESSAGE = "广播消息";

    @Test
    public void sendMessage() throws IOException, TimeoutException {
        Channel channel = RabbitmqConfig.getChannel();
        /**
         * 声明一个Exchange
         * exchange：名称
         * type：类型
         */
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.FANOUT);
        /**
         * 发送消息
         * exchange：交换机名称
         * routingKey：因为Exchange类型为fanout，所以routingkey没有意义，因此为空
         */
        channel.basicPublish(EXCHANGE,"", null,MESSAGE.getBytes("utf-8"));
        channel.close();
        channel.getConnection().close();
    }

}
