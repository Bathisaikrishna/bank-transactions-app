package com.kafka.streams;

import com.kafka.streams.config.StreamConfiguration;
import com.kafka.streams.topology.BankBalanceTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class BankBalanceApp {

    public static void main(String[] args) {
        Properties configuration = StreamConfiguration.getConfiguration();
        Topology topology = BankBalanceTopology.buildTopology();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, configuration);

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
