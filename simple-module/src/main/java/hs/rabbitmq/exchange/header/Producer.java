package hs.rabbitmq.exchange.header;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import hs.rabbitmq.config.RabbitmqConfig;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbitmq-demo
 * @description: 消息生产者
 *              如这个案例，创建两个consumer，消息接受者，并且绑定Exchange和Queue之间的参数，参数如下
 *              1. flag=consumer, tag=test
 *              2. flag=consumer
 *              设绑定参数x-match=all，可以看出来，第一个的key/value包含第二个key/value，所以发送的消息header属性符合第一个必然属于第二个queue，
 *              反之，符合第二个queue却不符合第一个
 *              设绑定参数x-match=any，则符合第二个queue必然符合第一个queue，符合第一个queue则不一定符合第二个queue，因为可能发送的消息Header只符合第一个queue的tag参数，这样的话，就不符合
 *              第二个queue了
 * @create: 2021-01-28 22:05
 **/
public class Producer {

    private final String EXCHANGE = "MESSAGE_HEADER";

    @Test
    public void sendMessage() throws IOException, TimeoutException {
        Channel channel = RabbitmqConfig.getChannel();
        /**
         * 声明一个Exchange
         * exchange：名称
         * type：类型
         */
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.HEADERS);

        List<Map> headers = new ArrayList<>();
        Map<String,Object> header1 = new HashMap<>();
        header1.put("flag","consumer");
        header1.put("tag","test");
        headers.add(header1);
        Map<String,Object> header2 = new HashMap<>();
        header2.put("flag","consumer");
        headers.add(header2);
        Map<String,Object> header3 = new HashMap<>();
        header3.put("flag","consumer");
        header3.put("tag","test");
        header3.put("other","error");
        headers.add(header3);
        Map<String,Object> header4 = new HashMap<>();
        header4.put("tag","test");
        headers.add(header4);
        Map<String,Object> header5 = new HashMap<>();
        header5.put("flag","#");
        headers.add(header5);
        this.send(channel,headers);

        channel.close();
        channel.getConnection().close();
    }

    /**
     * 封装一个专门发送消息的方法
     * @param channel
     */
    private void send(Channel channel, List<Map> headers) throws IOException {

        Iterator iterator = headers.iterator();
        while (iterator.hasNext()){
            Map<String,Object> header = (Map<String,Object>)iterator.next();
            // 定义消息配置信息，这里主要定义消息Header信息
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().headers(header).build();
            Iterator<String> keys = header.keySet().iterator();
            StringBuffer message = new StringBuffer();
            while (keys.hasNext()){
                String key = keys.next();
                message.append(key+"="+header.get(key)+"，");
            }
            /**
             * 发送消息
             * exchange：交换机名称
             * routingKey：Exchange type 是 Header，则routingkey为空
             * props：定义消息配置属性信息，比如 Routing Header
             */
            channel.basicPublish(EXCHANGE,"", properties,message.toString().getBytes());
        }
    }
}
