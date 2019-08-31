package xyz.fz.rocketmq.configuration.rocketmq;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

public class RocketmqProducer extends DefaultMQProducer {

    public RocketmqProducer(String namesrvAddr, String group, String instanceName) {
        super(group);
        this.setNamesrvAddr(namesrvAddr);
        this.setInstanceName(instanceName);
    }

    public void init() throws MQClientException {
        this.start();
    }

    public void destroy() {
        this.shutdown();
    }
}
