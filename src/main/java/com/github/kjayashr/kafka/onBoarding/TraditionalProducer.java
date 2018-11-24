package com.github.kjayashr.kafka.onBoarding;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TraditionalProducer {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(TraditionalProducer.class);

        String bootstrapProperties = "127.0.0.1:9092";

        //producer properties- the traditional way
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapProperties);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        for (int i = 800; i<910;i++) {
            String topic = "testt_topic2";
            String key = "id_" + Integer.toString(i);
            String value = "message "+Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received meta data \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "TimeStamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing a message: " + e.getStackTrace());

                    }
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
