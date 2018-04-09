package com.openbet.kafta.dao.producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaProducer {

    private final static String TOPIC = "customers-aux";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    private final static String CLIENT_ID_CONFIG = "KafkaProducer";
    private final static Logger log = LoggerFactory.getLogger(KafkaProducer.class);
    private static Producer<Long,String> producer;


    public KafkaProducer(){
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new org.apache.kafka.clients.producer.KafkaProducer(props);
    }

    public void sendRecord(ConsumerRecord<Long, String> prevRecord, String value){
        long time = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(100);

        try {
                final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, prevRecord.key(), value);
                producer.send(record, (metadata, exception) -> {
                    long elapsedTime = System.currentTimeMillis() - time;
                    if (metadata != null) { System.out.printf("sent record(key=%s value=%s) " +
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

/*
    public void run(final int sendMessageCount) throws InterruptedException {


        long time = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);

        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<Long, String> record =
                        new ProducerRecord<>(TOPIC, index, "{ \"Customer.id\": \"123\", \"Customer.username\": \"123\", \"Customer.status\": \"123\", \"Customer.langRef\": \"123\", \"Customer.PersonalDetails.firstName\": \"123\", \"Customer.PersonalDetails.lastName\": \"123\", \"Customer.Regisration.source\": \"123\", \"Customer.Registration.countryRef\": \"CAN\", \"Customer.CustomerAddress.Address.stateRef\": \"123\" or null, \"Customer.CustomerFlags\": \"123\", \"Customer.CustomerGroups\": \"123\", \"Customer.Account.currencyRef\": \"123\" }" + index);
                producer.send(record, (metadata, exception) -> {
                    long elapsedTime = System.currentTimeMillis() - time;
                    if (metadata != null) { System.out.printf("sent record(key=%s value=%s) " +
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
            }
            countDownLatch.await(25, TimeUnit.SECONDS);
        } finally {
            producer.flush();
        }
    }

    public static void main(String... args) throws Exception {
        KafkaProducer producer = new KafkaProducer();
        producer.run(Integer.MAX_VALUE);
    }
    */
}
