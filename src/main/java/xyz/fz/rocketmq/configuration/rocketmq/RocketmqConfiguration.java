package xyz.fz.rocketmq.configuration.rocketmq;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class RocketmqConfiguration {

    @Value("${rocketmq.namesrv.addr}")
    private String namesrvAddr;

    @Value("${rocketmq.topic.a}")
    private String topicA;

    @Value("${rocketmq.producer.group}")
    private String producerGroup;

    @Value("${rocketmq.producer.instanceName}")
    private String producerInstanceName;

    @Value("${rocketmq.producer.trans.group}")
    private String producerTransGroup;

    @Value("${rocketmq.producer.trans.instanceName}")
    private String producerTransInstanceName;

    @Value("${rocketmq.consumer.group}")
    private String consumerGroup;

    @Value("${rocketmq.consumer.instanceName}")
    private String consumerInstanceName;

    @Bean(name = "rocketmqProducer", initMethod = "init", destroyMethod = "destroy")
    public RocketmqProducer rocketmqProducer() {
        return new RocketmqProducer(namesrvAddr, producerGroup, producerInstanceName);
    }

    @Bean(name = "rocketmqTransactionProducer", initMethod = "init", destroyMethod = "destroy")
    public RocketmqTransactionProducer rocketmqTransactionProducer() {
        return new RocketmqTransactionProducer(namesrvAddr, producerTransGroup, producerTransInstanceName, new TransactionListener() {
            private AtomicInteger transactionIndex = new AtomicInteger(0);

            private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                int value = transactionIndex.getAndIncrement();
                int status = value % 3;
                localTrans.put(msg.getTransactionId(), status);
                System.out.println("exec [msgTransId: " + msg.getTransactionId() + ", status: " + status + "]");
                return LocalTransactionState.UNKNOW;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                Integer status = localTrans.get(msg.getTransactionId());
                System.out.println("check [msgTransId: " + msg.getTransactionId() + ", status: " + status + "]");
                if (null != status) {
                    switch (status) {
                        case 0:
                            return LocalTransactionState.UNKNOW;
                        case 1:
                            return LocalTransactionState.COMMIT_MESSAGE;
                        case 2:
                            return LocalTransactionState.ROLLBACK_MESSAGE;
                    }
                }
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
    }

    @Bean(name = "rocketmqConsumer", initMethod = "init", destroyMethod = "destroy")
    public RocketmqConsumer rocketmqConsumer() throws MQClientException {
        return new RocketmqConsumer(
                namesrvAddr,
                consumerGroup,
                consumerInstanceName,
                topicA,
                "*",
                (messages, context) -> {
                    int count = 0;
                    for (MessageExt message : messages) {
                        String content = "msgId=" + message.getMsgId() + ", msgBody=" + new String(message.getBody());
                        long delay = System.currentTimeMillis() - message.getStoreTimestamp();
                        String curThreadName = Thread.currentThread().getName();
                        System.out.println("Thread[" + curThreadName + "]" + " receive message" + (count++) + "[" + content + "] " + delay + "ms later.");
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                });
    }
}
