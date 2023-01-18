package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootstrapServers = "127.0.0.1:9092";

        // 1. Properties section
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            // 3. Create a produce record.
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first_topic", "hello World"+ i);

            // 4. Send Data - Asynchronous
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("\n\n>>>> Received new metadata. <<<< \n" +
                            "-------------------------------- \n" +
                            "- Topic:" + recordMetadata.topic() + "\n" +
                            "- Partition:" + recordMetadata.partition() + "\n" +
                            "- Offset:" + recordMetadata.offset() + "\n" +
                            "- Timestamp:" + recordMetadata.timestamp() + "\n\n");
                } else {
                    logger.error("Error while producing", e);
                }
            });
        }
        // 5. flush and close
        producer.flush();
        producer.close();

    }
}
