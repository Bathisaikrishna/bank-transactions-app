package com.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.streams.model.BankTransaction;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.*;

public class BankTransactionProducer {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<Long, String> bankTransactionProducer =
                new KafkaProducer<>(props);


        List<BankTransaction> data1 = Arrays.asList(
                BankTransaction.builder()
                        .balanceId(1L)
                        .time(new Date())
                        .amount(new BigDecimal(500))
                        .build(),
                BankTransaction.builder()
                        .balanceId(2L)
                        .time(new Date())
                        .amount(new BigDecimal(3000)).build(),
                BankTransaction.builder()
                        .balanceId(1L)
                        .time(new Date())
                        .amount(new BigDecimal(500)).build(),
                BankTransaction.builder()
                        .balanceId(4L)
                        .time(new Date())
                        .amount(new BigDecimal(2000)).build(),
                BankTransaction.builder()
                        .balanceId(4L)
                        .time(new Date())
                        .amount(new BigDecimal(-2500)).build(),
                BankTransaction.builder()
                        .balanceId(3L)
                        .time(new Date())
                        .amount(new BigDecimal(1000)).build(),
                BankTransaction.builder()
                        .balanceId(1L)
                        .time(new Date())
                        .amount(new BigDecimal(-500)).build(),
                BankTransaction.builder()
                        .balanceId(2L)
                        .time(new Date())
                        .amount(new BigDecimal(-4000)).build(),
                BankTransaction.builder()
                        .balanceId(3L)
                        .time(new Date())
                        .amount(new BigDecimal(-500)).build()
        );
        data1.stream()
                .map(bankTransaction -> new ProducerRecord<>("bank-transactions", bankTransaction.getBalanceId(), toJson(bankTransaction)))
                .forEach(record -> send(bankTransactionProducer, record));

        BankTransaction bankTransaction = BankTransaction.builder()
                .balanceId(3L)
                .time(new Date())
                .amount(new BigDecimal(-10_000)).build();

        send(bankTransactionProducer, new ProducerRecord<>("bank-transactions", bankTransaction.getBalanceId(), toJson(bankTransaction)));

    }

    @SneakyThrows
    private static void send(KafkaProducer<Long, String> bankTransactionProducer, ProducerRecord<Long, String> record) {
        bankTransactionProducer.send(record).get();
    }

    @SneakyThrows
    private static String toJson(BankTransaction bankTransaction) {
        return OBJECT_MAPPER.writeValueAsString(bankTransaction);
    }
}
