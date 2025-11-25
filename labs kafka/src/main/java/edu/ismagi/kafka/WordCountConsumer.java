package edu.ismagi.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class WordCountConsumer {
    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("Entrer le nom du topic");
            return;
        }

        String topic = args[0];

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("group.id", "wordcount-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        HashMap<String, Integer> counts = new HashMap<>();

        System.out.println("Consommateur démarré...");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {

                String word = record.value();
                counts.put(word, counts.getOrDefault(word, 0) + 1);

                System.out.println("Mot: " + word + " | Compte: " + counts.get(word));
            }
        }
    }

}
