package hs.rabbitmq.springbootmodule.Routing;

import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * @author heshuai
 * @title: RoutingConsumer
 * @description: Routing模式，根据RoutingKey来选择性地将消息发送到某些Queue
 * @date 2021年02月09日 9:46
 */
@Component
public class RoutingConsumer {

    @RabbitListener(bindings = @QueueBinding(value = @Queue,
                                            exchange = @Exchange(name = "DIRECT_ROUTING"), // 默认类型Direct
                                            key = {"info","warn"}))
    public void receive1(String message) {
        System.out.printf("receive1 收到消息如下：\n %s \n",message);
    }

    @RabbitListener(bindings = @QueueBinding(value = @Queue,
            exchange = @Exchange(name = "DIRECT_ROUTING"), // 默认类型Direct
            key = {"info"}))
    public void receive2(String message) {
        System.out.printf("receive2 收到消息如下：\n %s \n",message);
    }
    @RabbitListener(bindings = @QueueBinding(value = @Queue,
            exchange = @Exchange(name = "DIRECT_ROUTING"), // 默认类型Direct
            key = {"error"}))
    public void receive3(String message) {
        System.out.printf("receive3 收到消息如下：\n %s \n",message);
    }
}
