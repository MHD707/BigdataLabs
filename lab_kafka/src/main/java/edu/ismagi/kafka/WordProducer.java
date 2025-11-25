package edu.ismagi.kafka;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WordProducer {
    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("Entrer le nom du topic");
            return;
        }

        String topic = args[0];

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Scanner scanner = new Scanner(System.in);
        System.out.println("Saisir du texte :");

        while (true) {
            String line = scanner.nextLine();
            for (String word : line.split("\\s+")) {
                producer.send(new ProducerRecord<>(topic, word, word));
                System.out.println("Envoyé: " + word);
            }
        }
    }
}
