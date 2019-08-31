package xyz.fz.rocketmq.configuration.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;

public class RocketmqConsumer extends DefaultMQPushConsumer {

    public RocketmqConsumer(
            String namesrvAddr,
            String group,
            String instanceName,
            String topic,
            String topicSubExp,
            MessageListenerConcurrently messageListenerConcurrently) throws MQClientException {
        super(group);
        this.setNamesrvAddr(namesrvAddr);
        this.setInstanceName(instanceName);
        this.subscribe(topic, topicSubExp);
        this.setConsumeMessageBatchMaxSize(32);
        this.registerMessageListener(messageListenerConcurrently);
    }

    public void init() throws MQClientException {
        this.start();
    }

    public void destroy() {
        this.shutdown();
    }
}
