package hs.rabbitmq.workQueues;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import hs.rabbitmq.config.RabbitmqConfig;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbitmq-demo
 * @description: 消息生产者
 * @create: 2021-01-04 21:47
 **/
public class Producer {

    @Test
    public void sendManyM() throws IOException, TimeoutException {
        Channel channel = RabbitmqConfig.getChannel();
        // 声明work queue ，并将它设置为持久化
        channel.queueDeclare("work-queues",true,false,false,null);
        for (int i = 0;i<10;++i){
            String message = "第"+i+"条消息";
            channel.basicPublish("","work-queues", MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes("utf-8"));
        }
        channel.close();
        channel.getConnection().close();
    }
}
