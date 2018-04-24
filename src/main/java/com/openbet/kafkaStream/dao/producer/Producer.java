package com.openbet.kafkaStream.dao.producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Producer {

    private final String TOPIC = "topic";
    private final Logger log = LoggerFactory.getLogger(Producer.class);
    private final Properties props = new Properties();
    private org.apache.kafka.clients.producer.Producer producer;


    public Producer() {

        // Initialize properties
        init();

        // Create the producer using props.
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer(props);
    }

    public void sendRecord(ConsumerRecord<Long, String> prevRecord, String value) {
        long time = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(100);

        try {
            final ProducerRecord<Long, String> record = new ProducerRecord<>(props.getProperty(TOPIC), prevRecord.key(), value);
            producer.send(record, (metadata, exception) -> {
                long elapsedTime = System.currentTimeMillis() - time;
                if (metadata != null) {
                    System.out.printf("sent record(key=%s value=%s) " +
                                    "meta(partition=%d, offset=%d) time=%d\n",
                            record.key(), record.value(), metadata.partition(),
                            metadata.offset(), elapsedTime);
                    log.info("sent record(key=%s value=%s) " +
                                    "meta(partition=%d, offset=%d) time=%d\n",
                            record.key(), record.value(), metadata.partition(),
                            metadata.offset(), elapsedTime);
                } else {
                    exception.printStackTrace();
                }
                countDownLatch.countDown();
            });
        } finally {
            producer.flush();
        }
    }

    private void init() {
        InputStream input = null;

        try {

            input = new FileInputStream("./src/main/resources/config/KafkaTopicFrom");

            // load a properties file
            props.load(input);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

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
}
