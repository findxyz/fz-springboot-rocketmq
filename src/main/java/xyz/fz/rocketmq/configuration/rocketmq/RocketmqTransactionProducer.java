package xyz.fz.rocketmq.configuration.rocketmq;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RocketmqTransactionProducer extends TransactionMQProducer {

    private static int CPU_CORES = Runtime.getRuntime().availableProcessors();

    private static ExecutorService executorService = new ThreadPoolExecutor(
            CPU_CORES,
            CPU_CORES * 2,
            100,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(2000),
            r -> {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
    );

    public RocketmqTransactionProducer(String namesrvAddr,
                                       String group,
                                       String instanceName,
                                       TransactionListener transactionListener) {
        super(group);
        this.setNamesrvAddr(namesrvAddr);
        this.setInstanceName(instanceName);
        this.setExecutorService(executorService);
        this.setTransactionListener(transactionListener);
    }

    public void init() throws MQClientException {
        this.start();
    }

    public void destroy() {
        this.shutdown();
    }
}
