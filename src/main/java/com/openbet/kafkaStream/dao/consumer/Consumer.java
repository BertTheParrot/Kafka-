package com.openbet.kafkaStream.dao.consumer;

import com.openbet.kafkaStream.dao.producer.Producer;
import com.openbet.kafkaStream.utils.Filter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

public class Consumer implements Runnable {

    private final String TOPIC = "topic";
    private final Logger log = LoggerFactory.getLogger(Consumer.class);
    private final Properties props = new Properties();
    private org.apache.kafka.clients.consumer.Consumer consumer;

    private Filter filter;
    private Producer producer;

    public Consumer() throws IOException {

        // Initialize properties
        init();

        // Create the consumer using props.
        final org.apache.kafka.clients.consumer.Consumer consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(props.getProperty(TOPIC)));
        this.consumer = consumer;
        this.producer = new Producer();
        this.filter = new Filter();
        run();
    }

    public static void main(String args[]) throws Exception {

        Consumer consumer = new Consumer();
    }

    private void init() {
        InputStream input = null;

        try {

            input = new FileInputStream("./src/main/resources/config/KafkaTopicFrom");

            // load a properties file
            props.load(input);

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
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
                String customerInfo = record.value();
                try {
                    String filterInfo = filter.clean(customerInfo);
                    producer.sendRecord(record, filterInfo);
                } catch ( IOException e) {
                    log.error(e.toString());
                }


                System.out.printf("consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());

            }

            consumer.commitAsync();
        }
        consumer.close();
        log.info("Process finished with exit code 0");
    }
}