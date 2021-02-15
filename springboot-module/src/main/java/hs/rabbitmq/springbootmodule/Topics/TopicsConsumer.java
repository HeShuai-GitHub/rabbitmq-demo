package hs.rabbitmq.springbootmodule.Topics;

import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * @author heshuai
 * @title: TopicsConsumer
 * @description: Topics模式，模型匹配模式
 * @date 2021年02月09日 10:06
 */
@Component
public class TopicsConsumer {

    @RabbitListener(bindings = @QueueBinding(value = @Queue,
                                            exchange = @Exchange(name = "TOPIC_TOPICS",type = ExchangeTypes.TOPIC),
                                            key = {"log.*"}))
    public void receive1(String message){
        System.out.printf("receive1 收到消息如下\n %s \n",message);
    }

    @RabbitListener(bindings = @QueueBinding(value = @Queue,
            exchange = @Exchange(name = "TOPIC_TOPICS",type = ExchangeTypes.TOPIC),
            key = {"log.#"}))
    public void receive2(String message){
        System.out.printf("receive2 收到消息如下\n %s \n",message);
    }

    @RabbitListener(bindings = @QueueBinding(value = @Queue,
            exchange = @Exchange(name = "TOPIC_TOPICS",type = ExchangeTypes.TOPIC),
            key = {"log.info"}))
    public void receive3(String message){
        System.out.printf("receive3 收到消息如下\n %s \n",message);
    }
}
