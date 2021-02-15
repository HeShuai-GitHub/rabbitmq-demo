package hs.rabbitmq.springbootmodule.exchange.header;

import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.stereotype.Component;

/**
 * @author heshuai
 * @title: HeaderConsumer
 * @description: Exchange Header类型
 * @date 2021年02月12日 16:49
 */
@Component
public class HeaderConsumer {
    /**
     * arguments: Queue和Exchange绑定时所涉及到的参数，一般就是Exchange类型为Header才会用到
     * @Argument： 就是专门设置相对应的参数的，k-v的形式，
     * 在这个类型中，有一个特殊的参数就是x-match，它是用来定义Header的匹配形式，有两个值，分别是all（默认）、any，如果有多对k-v的参数的话，
     * 并且x-match值为all，那么必须匹配所有k-v参数才可以路由到这个queue；
     * x-match值为any，那么只需要匹配其中任意一个k-v参数就可以路由到这个queue了
     */
    @RabbitListener(bindings = @QueueBinding(value = @Queue,
                                            exchange = @Exchange(name = "EXCHANGE_HEADER", type = ExchangeTypes.HEADERS),
                                            arguments = {@Argument(name = "x-match", value = "all"),
                                                        @Argument(name = "key", value = "log.error"),
                                                        @Argument(name = "flag", value = "error")}))
    public void receive1(String message){
        System.out.printf("receive1 收到消息： %s \n",message);
    }

    @RabbitListener(bindings = @QueueBinding(value = @Queue,
            exchange = @Exchange(name = "EXCHANGE_HEADER", type = ExchangeTypes.HEADERS),
            arguments = {@Argument(name = "x-match", value = "any"),
                    @Argument(name = "key", value = "log.info"),
                    @Argument(name = "flag", value = "log")}))
    public void receive2(String message){
        System.out.printf("receive2 收到消息： %s \n",message);
    }

    @RabbitListener(bindings = @QueueBinding(value = @Queue,
            exchange = @Exchange(name = "EXCHANGE_HEADER", type = ExchangeTypes.HEADERS),
            arguments = {@Argument(name = "x-match", value = "all"),
                    @Argument(name = "key", value = "info.error"),
                    @Argument(name = "flag", value = "info")}))
    public void receive3(String message){
        System.out.printf("receive3 收到消息： %s \n",message);
    }

}
