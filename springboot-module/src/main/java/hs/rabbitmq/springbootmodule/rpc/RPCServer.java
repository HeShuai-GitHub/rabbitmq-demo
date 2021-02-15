package hs.rabbitmq.springbootmodule.rpc;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;


/**
 * @author heshuai
 * @title: RPCServer
 * @description: 在RPC模式中，消息的接受处理者一般也称之为服务端，接受处理响应请求的。
 * @date 2021年02月09日 10:22
 */
@Component
public class RPCServer {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    /**
     *
     * @param task
     * @param channel
     * @throws IOException
     */
    @RabbitListener(bindings = @QueueBinding(value = @Queue,
                                            exchange = @Exchange(value = "RPCSERVER", type = ExchangeTypes.TOPIC),
                                            key = {"task.#"}))
    public void process(String taskMessage, Message task, Channel channel) throws IOException {
        try {
            // 获取任务
            System.out.printf("收到任务如下：\n %s \n",taskMessage);
            // 模拟处理任务
            taskMessage +="，该任务已被处理";
            // 得到CorrelationId
            String correlationId = task.getMessageProperties().getCorrelationId();
            // 得到回调Queue
            String replyTo = task.getMessageProperties().getReplyTo();
            MessageProperties messageProperties = new MessageProperties();
            messageProperties.setCorrelationId(correlationId);
            // 定义Message;
            Message replyMessage = new Message(taskMessage.getBytes(),messageProperties);
            // 回复Response结果
            rabbitTemplate.send(replyTo, replyMessage);
            // 手动确认消息
            channel.basicAck(task.getMessageProperties().getDeliveryTag(),false);
        }catch (IOException e){
            channel.basicNack(task.getMessageProperties().getDeliveryTag(),false,true);
            throw e;
        }

    }
}
