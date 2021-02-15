package hs.rabbitmq.springbootmodule.publishAndSubscribe;

import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * @author heshuai
 * @title: SubscribeConsumer
 * @description: 发布订阅模式： 这个模式是基于AMQP协议中Exchange概念，所以等下主要是看Exchange
 *              @Component: 交给Spring管理
 * @date 2021年02月08日 16:45
 */
@Component
public class SubscribeConsumer {
    /**
     * 接受消息方法：
     *           @RabbitListener： bindings属性代表绑定Exchange和Queue，并且定义他们的RoutingKey
     *           @Exchange： 定义一个Exchange，根据它的模式定义，如果这个Exchange不存在则主动创建一个Exchange，type类型默认Direct。
     *                      并且在这个注解中支持x-delayed-message类型，默认 false
     * @param message
     */
    @RabbitListener(bindings = @QueueBinding(value = @Queue, // 生成一个临时Queue
                                            exchange = @Exchange(name = "PubScrExchange", type = ExchangeTypes.FANOUT),
                                            key = {"publish", "subscribe"})) // Fanout类型，是否使用key都可以
    public void receive(String message) {
        System.out.println("收到消息如下：");
        System.out.println(message);
    }
}
