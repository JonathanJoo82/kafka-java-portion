package io.conductor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.datatransfer.StringSelection;
import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("hello world");

        //create Producer proterties
        //kafka-console-producer.sh --producer.config playground.config --bootstrap-server cluster.playground.cdkt.io:9092 --topic first_topic
        /*
        each of the options that are added to the command of the script can be set in the object of the properties.
        when connecting to local
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        other wise to connect to a server all of the properties listed below must be configured.
         */
        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
//        properties.setProperty("security.protocol", "value");
//        properties.setProperty("sasl.jaas.config", "value");
//        properties.setProperty("saslmechanism", "value");


        //before sending it to kafka it will be serialized into bytes. both key and value
        properties.setProperty("key.serializer", StringSerializer.class.getSimpleName());
        properties.setProperty("value.serializer", StringSerializer.class.getSimpleName());


        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

        //send data
        producer.send(producerRecord);

        //flush(send all data and block until done) and close the producer
        producer.flush();
        producer.close();
    }
}
