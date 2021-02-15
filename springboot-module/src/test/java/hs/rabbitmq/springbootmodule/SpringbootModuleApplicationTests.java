package hs.rabbitmq.springbootmodule;

import com.alibaba.fastjson.JSON;
import net.minidev.json.JSONArray;
import org.apache.coyote.http11.filters.VoidOutputFilter;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.lang.Nullable;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SpringBootTest(classes = SpringbootModuleApplication.class)
class SpringbootModuleApplicationTests {
    /**
     * RabbitTemplate
     * SpringFramework 提供一个类似于RedisTemplate、RestTemplate这些的一个高度封装的模板，
     * 就是对于amqp-client的封装（没看过底层、猜测）
     * 简单介绍一下功能
     *      除了可以send、receive，还对这两个重载了很多方法，而且提供了更多如：convertAndSend、convertSendAndReceive这些更加深入的封装方法，
     *      基本上也就是对于发送消息和接受消息的各种角度的封装了；
     *      这个类中还包含一些特殊的默认值设置，比如：encoding、exchange、routingKey、defaultReceiveQueue，可以更加浓缩代码；
     *      这里有个必须设置的配置项就是connectionFactory，不过这个也是可以通过application.yml直接配置的，没什么区别
     *      除了这些以外还有一个事务的设置(setChannelTransacted())，这个就不深入了
     */
    @Autowired
    private RabbitTemplate rabbitTemplate;

    /**
     * HelloWord 模型 消息发布
     */
    @Test
    void helloWord() throws InterruptedException {
        /**
         * 为了介绍这个方法，使用这个方法重载最多参数的方法
         * exchange：交换机名字，“”就是默认交换机
         * routingKey：路由Key, 使用AMQP中的默认交换机，默认与所有的queue相绑定，queue名就是他们的绑定key
         * object：发送的消息内容，因为convertAndSend方法对这个进行了封装，所以可以直接发送对象，不局限byte数组了
         * CorrelationData：这个应该是用于发布者确认模型中所需要用到的参数
         */
        rabbitTemplate.convertAndSend("","helloWord",String.format("我是一个简简单单的HelloWord\n你好！这个世界！\n愿世间没有病痛\n"), (CorrelationData)null);
        TimeUnit.SECONDS.sleep(1L);
    }

    /**
     * WorkQueue 模型 消息发布
     */
    @Test
    void workQueue() throws InterruptedException {
        for (int i=0; i<10; i++){
            rabbitTemplate.convertAndSend("workQueue",String.format("这是第"+i+"条祝福\n"));
        }
        TimeUnit.SECONDS.sleep(2L);
    }

    /**
     * Publish/Subscribe模式
     *  在此模式中是否使用routingKey都不重要, 因为Fanout类型就是将消息广播到所有的Queue
     */
    @Test
    void publishExchange() throws InterruptedException {
        rabbitTemplate.convertAndSend("PubScrExchange","publish","我是Publish");
        rabbitTemplate.convertAndSend("PubScrExchange","subscribe","我是Subscribe");
        TimeUnit.SECONDS.sleep(1L);
    }

    /**
     * Routing模式
     */
    @Test
    void RoutingExchange() throws InterruptedException {
        rabbitTemplate.convertAndSend("DIRECT_ROUTING","info","我是info");
        rabbitTemplate.convertAndSend("DIRECT_ROUTING","warn","我是warn");
        rabbitTemplate.convertAndSend("DIRECT_ROUTING","error","我是error");
        TimeUnit.SECONDS.sleep(1L);
    }

    /**
     * Topics模式
     */
    @Test
    void TopicsExchange() throws InterruptedException {
        rabbitTemplate.convertAndSend("TOPIC_TOPICS","log.info","我是log.info");
        rabbitTemplate.convertAndSend("TOPIC_TOPICS","log.warn.system","我是log.warn.system");
        rabbitTemplate.convertAndSend("TOPIC_TOPICS","log.error","我是log.error");
        TimeUnit.SECONDS.sleep(1L);
    }

    /**
     * RPC 模式
     */
    @Test
    void RPCClient() throws InterruptedException {
        // convertSendAndReceive: 发送请求并等待相应结果，同步
        // 在这个demo中 因为server发送回来的response是byte[]类型，所以这个回复也是byte[]
        Object response = rabbitTemplate.convertSendAndReceive("RPCSERVER","task.info","我是一个简简单单的任务");
        System.out.printf("收到回复如下：\n %s \n",new String((byte[]) response));
        TimeUnit.SECONDS.sleep(1L);
    }
    /**
     * Publisher Confirms 模式
     */
    @Test
    void PublisherConfirmsClient() throws InterruptedException, TimeoutException, ExecutionException {
        // 为当前rabbitTemplate实例设置一个唯一的ConfirmCallback发布者确认异步回调方法
        this.rabbitTemplate.setConfirmCallback((CorrelationData correlationData, boolean ack,  String cause) ->{
            // 打印参数
            System.out.printf("\n CorrelationData：%s, \t cause：%s \n", JSON.toJSONString(correlationData),cause);
            if(ack){
                System.out.println("已确认");
            }else{
                System.out.println("消息未确认，需记录未确认原因及重新发送消息");
            }
        });
        String messageBody = "模拟发布者确认";
        Message message = new Message(messageBody.getBytes(),null);
        ReturnedMessage returned = new ReturnedMessage(message,1,null,null,null);
        // 发布者返回的数据，可以在这里记录发送的消息体，以备后续逻辑处理
        CorrelationData correlationData = new CorrelationData();
        correlationData.setReturned(returned);
        // 发送消息
        this.rabbitTemplate.convertAndSend("helloWord", (Object) "模拟发布者确认",correlationData);
        // 也可以不设置统一回调方法，而是使用这种简单的属性future来获取是否发布成功
        System.out.println(correlationData.getFuture().get(200,TimeUnit.MILLISECONDS).isAck());
        System.out.println(correlationData.getFuture().get(200,TimeUnit.MILLISECONDS).getReason());
        TimeUnit.SECONDS.sleep(1L);
    }
    /**
     * Exchange Header类型
     */
    @Test
    void exchangeHeader() throws InterruptedException {
        this.rabbitTemplate.convertAndSend("EXCHANGE_HEADER","","key=info.error,flag=error消息",
                (Message message) ->{
                    MessageProperties messageProperties = message.getMessageProperties();
                    messageProperties.setHeader("key","info.error");
                    messageProperties.setHeader("flag","info");
                    return message;
                });
        this.rabbitTemplate.convertAndSend("EXCHANGE_HEADER","","key=我是瞎写的，flag=log消息",
                (Message message) ->{
                    MessageProperties messageProperties = message.getMessageProperties();
                    messageProperties.setHeader("key","我是瞎写的");
                    messageProperties.setHeader("flag","log");
                    return message;
                });
        this.rabbitTemplate.convertAndSend("EXCHANGE_HEADER","","key=log.error，flag=info消息",
                (Message message) ->{
                    MessageProperties messageProperties = message.getMessageProperties();
                    messageProperties.setHeader("key","log.error");
                    messageProperties.setHeader("flag","error");
                    return message;
                });
        TimeUnit.SECONDS.sleep(1L);
    }
}
