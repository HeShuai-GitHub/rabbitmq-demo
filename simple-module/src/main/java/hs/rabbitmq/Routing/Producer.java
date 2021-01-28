package hs.rabbitmq.Routing;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import hs.rabbitmq.config.RabbitmqConfig;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbitmq-demo
 * @description: 消息生产者
 * @create: 2021-01-05 22:53
 **/
public class Producer {

    private final String EXCHANGE = "MESSAGE_DIRECT";

    @Test
    public void sendMessage() throws IOException, TimeoutException {
        Channel channel = RabbitmqConfig.getChannel();
        /**
         * 声明一个Exchange
         * exchange：名称
         * type：类型
         */
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT);
        /**
         * 发送消息
         * exchange：交换机名称
         * routingKey：DIRECT类型，必须需要routingKey
         */
        channel.basicPublish(EXCHANGE,"error", null,"报错信息".getBytes("utf-8"));
        channel.basicPublish(EXCHANGE,"info", null,"提示信息".getBytes("utf-8"));
        channel.basicPublish(EXCHANGE,"warm", null,"警告信息".getBytes("utf-8"));
        channel.close();
        channel.getConnection().close();
    }

}
