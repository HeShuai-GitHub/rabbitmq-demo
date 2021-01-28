package hs.rabbitmq.publisherConfirm;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import hs.rabbitmq.config.RabbitmqConfig;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
/**
 * @author heshuai
 * @title: PublisherConfirms
 * @description: Rabbitmq 官方实现Publisher Confirm（发布确认）实现，此模式为了提供了消息发布者可以确定消息到达Mq服务，如果再结合Server手动消息
 *               确认，那么就可以最大程度保证消息的不丢失，以下内容是Rabbitmq的实现方式，我只是对一下内容做必要的注释理解
 *               本案例比较简单，如果用于生产还需优化！！！
 * @date 2021年01月28日 19:44
 */
public class PublisherConfirms {
    // 官方测试50000条消息，太多了，这次测试1000
    static final int MESSAGE_COUNT = 1_000;

    public static void main(String[] args) throws Exception {
        publishMessagesIndividually();
        publishMessagesInBatch();
        handlePublishConfirmsAsynchronously();
    }

    /**
     * 单个消息同步确认实现发布者确认
     * 优点：实现简单
     * 缺点：发布消息受到影响，一秒内最多几百个发布
     * @throws Exception
     */
    static void publishMessagesIndividually() throws Exception {
        try (Connection connection = RabbitmqConfig.getConnection()) {
            Channel ch = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            // 声明一个队列
            ch.queueDeclare(queue, false, false, true, null);
            // 激活当前channel的发布者确认机制， 默认不启动
            ch.confirmSelect();
            // 开始时间戳
            long start = System.nanoTime();
            // 循环发布若干条消息进行测试
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                // 发布消息，采用默认Exchange
                ch.basicPublish("", queue, null, body.getBytes());

                /**
                 * 同步等待发布确认返回，可设置超时时长，时间单位是TimeUnit.MILLISECONDS
                 * 超时则抛出异常：TimeoutException
                 * nack-ed则抛出异常：IOException
                 */
                ch.waitForConfirmsOrDie(5_000);
            }
            // 结束时间戳
            long end = System.nanoTime();
            System.out.format("Published %,d messages individually in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    /**
     * 同步批量发布确认
     * 优点：发布数量大大提升，时间也较简单
     * 缺点：当出现nack-ed回复时，无法确认一批消息中哪条消息是nack-ed，所以若需要重新发布消息，则需要将一批所有消息进行重复发送
     * @throws Exception
     */
    static void publishMessagesInBatch() throws Exception {
        try (Connection connection = RabbitmqConfig.getConnection()) {
            Channel ch = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            ch.queueDeclare(queue, false, false, true, null);

            ch.confirmSelect();
            // 100消息为一组消息进行发送
            int batchSize = 100;
            // 当前发送消息的数量
            int outstandingMessageCount = 0;

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                ch.basicPublish("", queue, null, body.getBytes());
                outstandingMessageCount++;
                // 若整组消息发送完成后，同步等待发布确认，若这里出现了nack-ed，则可能无法确认是具体哪条消息出现了nack-ed
                if (outstandingMessageCount == batchSize) {
                    ch.waitForConfirmsOrDie(5_000);
                    outstandingMessageCount = 0;
                }
            }
            // 防止不满一组消息，在之前没有等待发布确认，这里处理一下
            if (outstandingMessageCount > 0) {
                ch.waitForConfirmsOrDie(5_000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages in batch in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    /**
     * 异步处理发布确认
     * 优点：灵活处理每一条消息发布确认结果，因为是异步，所以性能较好
     * 缺点：实现较复杂，涉及到复杂的场景时需要考虑的方面较多
     * @throws Exception
     */
    static void handlePublishConfirmsAsynchronously() throws Exception {
        try (Connection connection = RabbitmqConfig.getConnection()) {
            Channel ch = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            ch.queueDeclare(queue, false, false, true, null);

            ch.confirmSelect();
            /**
             * 并发环境下map集合，可序列化排序key
             */
            ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();
            /**
             * 发布确认异步回调方法
             */
            ConfirmCallback cleanOutstandingConfirms = (sequenceNumber, multiple) -> {
                /**
                 *  sequenceNumber：序列数字，关联消息和发布确认之间的
                 *  multiple: false:一条消息被ack/nack； true：所有低于等于当前sequenceNumber的消息被ack/nack
                 */
                if (multiple) {
                    // 为true，则查看集合outstandingConfirms中所有低于或等于当前sequenceNumber的消息，并将它从集合中清除
                    ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(
                            sequenceNumber, true
                    );
                    confirmed.clear();
                } else {
                    // 移除单条消息
                    outstandingConfirms.remove(sequenceNumber);
                }
            };
            /**
             * 在channel上添加一个发布确认的监听器
             * 分为ack处理和nack-ed处理
             * 第一个为ack处理回调，第二个为nack-ed处理回调
             */
            ch.addConfirmListener(cleanOutstandingConfirms, (sequenceNumber, multiple) -> {
                // nack-ed 回调处理逻辑

                // 在集合中获得nack-ed消息
                String body = outstandingConfirms.get(sequenceNumber);
                // 记录当前nack-ed消息日志
                System.err.format(
                        "Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n",
                        body, sequenceNumber, multiple
                );
                // 在集合中移除对应消息
                cleanOutstandingConfirms.handle(sequenceNumber, multiple);
            });

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                // 将对应消息和发布消息唯一标识关联起来并且存储在集合中
                outstandingConfirms.put(ch.getNextPublishSeqNo(), body);
                ch.basicPublish("", queue, null, body.getBytes());
            }
            // 超过60秒未全部返回确认消息，则报错
            if (!waitUntil(Duration.ofSeconds(60), () -> outstandingConfirms.isEmpty())) {
                throw new IllegalStateException("All messages could not be confirmed in 60 seconds");
            }

            long end = System.nanoTime();
            System.out.format("Published %,d messages and handled confirms asynchronously in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    /**
     * 等待若干时间
     * @param timeout 持续等待市场
     * @param condition 额外判断条件
     * @return
     * @throws InterruptedException
     */
    static boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        int waited = 0;
        // 每一百毫秒查看一下判断条件是否成立，若成立则返回；最后持续timeout所包含的持续时长
        while (!condition.getAsBoolean() && waited < timeout.toMillis()) {
            Thread.sleep(100L);
            waited = +100;
        }
        return condition.getAsBoolean();
    }

}