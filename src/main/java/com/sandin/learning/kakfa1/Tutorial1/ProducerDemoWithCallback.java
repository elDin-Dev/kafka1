package com.sandin.learning.kakfa1.Tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServer = "127.0.0.1:9092";

        //Create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++) {

            //Create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first_topic", "hello world! " + i );

            //Send Data - asynchronous
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
                                "Timestamp: " + recordMetadata.timestamp() + " \n"+
                                recordMetadata.offset()
                        );
                    }
                    else{
                        logger.error("error while producinc", e);
                    }
                }
            });
        }



        //flush data
        producer.flush();

        //flush adn close
        producer.close();


    }
}
