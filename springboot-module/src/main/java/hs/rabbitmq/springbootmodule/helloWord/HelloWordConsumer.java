package hs.rabbitmq.springbootmodule.helloWord;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.stereotype.Component;

/**
 * @author heshuai
 * @title: HelloWordConsumer
 * @description: HelloWord模型 消息消费者
 *              这里需要介绍几个注解
 *                  @Component： Spring IOC注解，注入当前对象实例到ICO容器
 *                  @RabbitListener： Rabbitmq监听器，监听Queue来接受消息，这个注解中包含了常用的接受消息时所涉及到的方法
 *                              id：唯一标识该监听器，可以自动生成，无需特殊设置
 *                              containerFactory：容器工厂，生成RabbitListener容器所用，一般使用默认工厂即可
 *                              queues：所监听Queue的名称，可以为数组，即监听多个Queue；注：所指定Queue必须存在，或者在其它位置开始声明对应的bean
 *                              queuesToDeclare：声明并监听该Queue，可以使用@Queue()去描述Queue
 *                              exclusive：true，单一消息中独占Queue，并且要求并发量为1；false:默认
 *                              bindings: 绑定Exchange和Queue，@QueueBinding可以使用这个注解去定义QueueBinding对象
 *                   @Queue： 定义一个Queue对象，这个注解在queuesToDeclare属性中可以使用
 *                              在这个对象中，可以定义一个常规的Queue的所有配置，这个和之前Simple-module中的内容基本一致
 * @date 2021年01月29日 11:17
 */
@Component
@RabbitListener(queuesToDeclare = @Queue("helloWord"))
public class HelloWordConsumer {
    /**
     *  @RabbitListener 可以使用在类上或者是方法上，如果使用在类上，需要指定类中的一个方法作为接受消息的方法，
     *                  所以这里使用了@RabbitHandler
     *  可以使用Object去接收消息，也可以使用String去接收：如果使用Object，那么接受的是Message对象，里面包含消息体、消息头等参数信息（不可以使用Message对象去直接接受）
     *  如果使用String去接收，则只是接收到消息体
     * @param message
     */
    /*
    @RabbitHandler
    public void receive(Object message){
        System.out.println("收到消息如下：");
        System.out.println(new String(((Message)message).getBody()));
    }*/

    @RabbitHandler
    public void receive(String message){
        System.out.println("收到消息如下：");
        System.out.println(message);
    }
}
