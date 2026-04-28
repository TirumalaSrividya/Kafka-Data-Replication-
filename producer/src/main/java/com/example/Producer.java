package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.commons.cli.*;

import java.util.Properties;
import java.util.UUID;

public class Producer {
    private static final String TOPIC = "commit-log";
    private static final String BOOTSTRAP_SERVERS = "primary-kafka:9092";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        Options options = new Options();
        options.addRequiredOption(null, "count", true, "Number of messages to produce");

        CommandLineParser parser = new DefaultParser();
        int count = 0;
        try {
            CommandLine cmd = parser.parse(options, args);
            count = Integer.parseInt(cmd.getOptionValue("count"));
        } catch (ParseException | NumberFormatException e) {
            System.err.println("Error parsing arguments: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("kafka-producer", options);
            System.exit(1);
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Producing " + count + " messages to " + TOPIC + " topic...");
            for (int i = 0; i < count; i++) {
                String event = generateEvent();
                producer.send(new ProducerRecord<>(TOPIC, event));
            }
            producer.flush();
            System.out.println("Successfully sent " + count + " messages!");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    private static String generateEvent() throws Exception {
        ObjectNode event = mapper.createObjectNode();
        event.put("event_id", UUID.randomUUID().toString());
        event.put("timestamp", System.currentTimeMillis());
        event.put("source", "transaction_service");
        event.put("op_type", "CREATE");
        ObjectNode payload = mapper.createObjectNode();
        payload.put("status", "success");
        payload.put("amount", 100);
        event.set("payload", payload);
        return mapper.writeValueAsString(event);
    }
}
