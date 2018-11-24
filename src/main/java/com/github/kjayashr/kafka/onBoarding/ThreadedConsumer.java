package com.github.kjayashr.kafka.onBoarding;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class ThreadedConsumer {
    public static void main(String[] args) {
        new ThreadedConsumer().run();
    }

    private ThreadedConsumer() {
    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ThreadedConsumer.class.getName());
        String bootstrapProperties = "127.0.0.1:9092";
        String group_id = "consumer-group-03-007";
        String topic = "testt_topic2";
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating the consumer thread...");
        Runnable consumerRunnableThread = new ConsumerRunnable(bootstrapProperties, group_id, topic, latch);
        Thread consumerThread = new Thread(consumerRunnableThread);
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook! ");
            ((ConsumerRunnable) consumerRunnableThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application exited");
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application is interrupted" + e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        public ConsumerRunnable(String bootStrapServers, String groupd_id, String topic, CountDownLatch latch) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupd_id);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord record : records) {
                        logger.info("\nKey: " + record.key() + "Value: " + record.value());
                        logger.info("\nPartition: " + record.partition() + "Offset:\n\n" + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received Shut Down Signal!! ");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }
        public void shutdown() {
            consumer.wakeup();
        }
    }
}
