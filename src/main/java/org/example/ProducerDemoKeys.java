package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServers = "127.0.0.1:9092";

        // 1. Properties section
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "hello World" + i;
            String key = "id_"+i;
            // 3. Create a produce record.
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic,key, value);

            logger.info("\n\n>>>> Key: "+key);
            // 4. Send Data - Asynchronous
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("\n> Received new metadata. < \n" +
                            "----------------------------- \n" +
                            "- Topic:" + recordMetadata.topic() + "\n" +
                            "- Partition:" + recordMetadata.partition() + "\n" +
                            "- Offset:" + recordMetadata.offset() + "\n" +
                            "- Timestamp:" + recordMetadata.timestamp() + "\n\n");
                } else {
                    logger.error("Error while producing", e);
                }
            }).get(); // block the .send() to make it synchronous - DON'T DO THIS IN PRODUCTION!
        }
        // 5. flush and close
        producer.flush();
        producer.close();

    }
}
