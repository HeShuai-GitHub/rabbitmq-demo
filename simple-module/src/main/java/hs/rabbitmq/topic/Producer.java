package hs.rabbitmq.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import hs.rabbitmq.config.RabbitmqConfig;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbitmq-demo
 * @description: 消息生产者
 * @create: 2021-01-06 00:10
 **/
public class Producer {

    private final String EXCHANGE = "MESSAGE_TOPIC";

    @Test
    public void sendMessage() throws IOException, TimeoutException {
        Channel channel = RabbitmqConfig.getChannel();
        /**
         * 声明一个Exchange
         * exchange：名称
         * type：类型
         */
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC);
        /**
         * 发送消息
         * exchange：交换机名称
         * routingKey：DIRECT类型，必须需要routingKey
         */
        channel.basicPublish(EXCHANGE,"log.error", null,"报错信息".getBytes("utf-8"));
        channel.basicPublish(EXCHANGE,"log.info", null,"提示信息".getBytes("utf-8"));
        channel.basicPublish(EXCHANGE,"log.warm", null,"警告信息".getBytes("utf-8"));
        channel.close();
        channel.getConnection().close();
    }
}
