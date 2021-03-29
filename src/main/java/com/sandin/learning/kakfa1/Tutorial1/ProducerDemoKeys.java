package com.sandin.learning.kakfa1.Tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServer = "127.0.0.1:9092";

        //Create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++) {

            String topic = "first_topic";
            String value = "hello world! " + i;
            String key  = "id_" + Integer.toString(i);

            //Create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value  );

            logger.info("key: " + key); //log the key

            //Send Data - synchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //it is executed every time a recoed is successfully sent or an exception is thrown
                    if (e == null) {
                        //succesfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + " \n"+
                                "Partition: " + recordMetadata.partition() + " \n"+
                                "Offset: " + recordMetadata.offset() + " \n"+
                                "Timestamp: " + recordMetadata.timestamp()
                        );
                    }
                    else{
                        logger.error("error while producinc", e);
                    }
                }
            }).get(); //Block the .send to be synchronous - don't do this in production
        }



        //flush data
        producer.flush();

        //flush adn close
        producer.close();


    }
}
