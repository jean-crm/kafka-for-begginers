package org.example;

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

public class ConsumerDemoWithThread {

    public static final int CONSUMER_POLL_DURATION = 100;

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    public void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                ()->{
                    logger.info("Caught shutdown hook");
                    ((ConsumerRunnable)myConsumerRunnable).shutdown();
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    logger.info("Application has exited");
                }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerRunnable implements Runnable{

        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;
        private final String autoOffsetResetConfig = "earliest";

        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch){
            this.latch = latch;
            // 1. Create consumer configuration
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);
            // 2. Create the consumer
            consumer = new KafkaConsumer<>(properties);
            // 3. Subscribe the consumer to a topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            // 4. Poll for new data
            try{
                while(true){
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(CONSUMER_POLL_DURATION)); // new in Kafka 2.0.0
                    for (ConsumerRecord<String, String> record: records) {
                        logger.info("Key: "+record.key()+", Value: "+record.value());
                        logger.info("Partition: "+record.partition()+", Offset: "+ record.offset());
                    }
                }
            } catch (WakeupException e){
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // tell our main code we're done with the consumer.
                latch.countDown();
            }
        }

        public void shutdown(){
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
