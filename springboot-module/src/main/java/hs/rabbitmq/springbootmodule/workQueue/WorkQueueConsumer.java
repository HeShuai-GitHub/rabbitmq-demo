package hs.rabbitmq.springbootmodule.workQueue;

import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * @author heshuai
 * @title: WorkQueueConsumer
 * @description: 工作模型——WorkQueue
 *                  因为需要在一个类中写多个监听处理，所以将@RaabitListener直接写到方法上机课
 * @date 2021年02月02日 11:05
 */
@Component
public class WorkQueueConsumer {
    /**
     * @RabbitListener: 可以直接添加到方法上，表示该方法就是Rabbitmq监听器处理函数
     */
    @RabbitListener(queuesToDeclare = @Queue(name = "workQueue"))
    public void workQueue1(String message){
        System.out.println("workQueue1收到消息如下：");
        System.out.println(message);
    }

    @RabbitListener(queuesToDeclare = @Queue(name = "workQueue"))
    public void workQueue2(String message){
        System.out.println("workQueue2收到消息如下：");
        System.out.println(message);
    }

}
