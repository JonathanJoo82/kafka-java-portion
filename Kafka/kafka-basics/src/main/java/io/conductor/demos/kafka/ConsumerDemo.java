package io.conductor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("hello world");
        String groudID = "my-java-application-comsumer-id";
        String topic = "demo-java";

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

        //create consumer config
        //takes the code and deserialize is and creates objects for the consumer to use.
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groudID);

        //none is where there is no group id it will fail
        //earliest is at the start of the topic
        //latest where it left off
        properties.setProperty("auto.offset.reset", "none/earliest/latest");

        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //pull the data from the topic

        //this example is a infiniate loop will continue to pull data from Kafka topics. 
        while(true){
            log.info("pulling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record: records){
                log.info(record.key());
            }
        }
    }
}
