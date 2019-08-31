package xyz.fz.rocketmq.controller;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import xyz.fz.rocketmq.configuration.rocketmq.RocketmqProducer;
import xyz.fz.rocketmq.configuration.rocketmq.RocketmqTransactionProducer;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/msg")
public class MessageController {

    private AtomicInteger counter = new AtomicInteger(0);

    @Value("${rocketmq.topic.a}")
    private String topicA;

    @Resource
    private RocketmqProducer rocketmqProducer;

    @Resource
    private RocketmqTransactionProducer rocketmqTransactionProducer;

    @RequestMapping("/send")
    public String send() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        SendResult sendResult = rocketmqProducer.send(new Message(topicA, ("你好 Rocketmq " + counter.getAndIncrement()).getBytes()));
        if (sendResult.getSendStatus().equals(SendStatus.SEND_OK)) {
            System.out.println(sendResult);
            return "ok";
        } else {
            return "not ok";
        }
    }

    @RequestMapping("/send/batch")
    public String sendBatch() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        List<Message> messageList = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            messageList.add(new Message(topicA, ("你好 Rocketmq " + counter.getAndIncrement()).getBytes()));
        }
        SendResult sendResult = rocketmqProducer.send(messageList);
        if (sendResult.getSendStatus().equals(SendStatus.SEND_OK)) {
            System.out.println(sendResult);
            return "ok";
        } else {
            return "not ok";
        }
    }

    @RequestMapping("/send/trans")
    public String sendTrans() throws MQClientException {
        SendResult sendResult = rocketmqTransactionProducer.sendMessageInTransaction(new Message(topicA, ("你好 Rocketmq Transaction " + counter.getAndIncrement()).getBytes()), null);
        if (sendResult.getSendStatus().equals(SendStatus.SEND_OK)) {
            System.out.println(sendResult);
            return "ok";
        } else {
            return "not ok";
        }
    }
}
