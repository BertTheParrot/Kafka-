package com.openbet.kafkaStream.daoSimEndpoints;

import com.openbet.kafkaStream.dao.producer.Producer;
import com.openbet.kafkaStream.utils.Filter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumeraux implements Runnable {

    private final static String TOPIC = "customers-aux";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    private final static String CLIENT_ID_CONFIG = "Consumer";
    private final static Logger log = LoggerFactory.getLogger(KafkaConsumeraux.class);
    private static Producer producer;
    private static Consumer consumer;
    private static Filter filter;

    public KafkaConsumeraux() throws IOException {
        final Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        this.consumer = consumer;
        this.producer = new Producer();
        this.filter = new Filter();
    }

    public static void main(String... args) throws IOException {
        KafkaConsumeraux b = new KafkaConsumeraux();
        b.run();
    }

    public void run() {

        final int giveUp = 100;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            for (ConsumerRecord<Long, String> record : consumerRecords) {

                System.out.printf("consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());

                //log.info("consumer Record:(%d, %s, %d, %d)\n",record.key(), record.value(),record.partition(), record.offset());
            }

            consumer.commitAsync();
        }
        consumer.close();
        log.info("Process finished with exit code 0");
    }
}