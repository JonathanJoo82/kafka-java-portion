package io.conductor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWIthCallbacks {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWIthCallbacks.class.getSimpleName());

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
        //create key. Note that this way is not good. iterate it so that it can be placed in the proper partitions
        String key = "1";
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", key, "hello world");

        //send data
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                //executes every time a record is sent or an exception is made.
                if (exception == null){
                    //was sent successfully
                    log.info("message sent by the producer" + metadata.topic() + metadata.partition() + metadata.offset());
                } else {
                    log.error("exception");
                }
            }
        });

        //flush(send all data and block until done) and close the producer
        producer.flush();
        producer.close();

        //call backs are used to confirm the partition and offset the message was sent
    }
}
