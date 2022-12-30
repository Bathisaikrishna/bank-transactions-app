package com.kafka.streams.topology;

import com.kafka.streams.model.BankBalance;
import com.kafka.streams.model.BankTransaction;
import com.kafka.streams.model.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class BankBalanceTopology {

    public static final String BANK_TRANSACTIONS = "bank-transactions";
    public static final String BANK_BALANCES = "bank-balances";
    public static final String REJECTED_TRANSACTIONS = "rejected-transactions";

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        JsonSerde<BankTransaction> bankTransactionJsonSerde = new JsonSerde<>(BankTransaction.class);
        JsonSerde<BankBalance> bankBalanceJsonSerde = new JsonSerde<>(BankBalance.class);

        KStream<Long, BankBalance> bankBalanceKStream = streamsBuilder.stream(BANK_TRANSACTIONS, Consumed.with(Serdes.Long(), bankTransactionJsonSerde))
                .groupByKey()
                .aggregate(BankBalance::new, (aLong, bankTransaction, bankBalance) -> bankBalance.process(bankTransaction),
                        Materialized.with(Serdes.Long(), bankBalanceJsonSerde))
                .toStream();

        bankBalanceKStream.to(BANK_BALANCES, Produced.with(Serdes.Long(), bankBalanceJsonSerde));

        bankBalanceKStream.mapValues(BankBalance::getLatestTransaction)
                .filter((key, value) -> value.getState().equals(BankTransaction.BankTransactionState.REJECTED))
                .to(REJECTED_TRANSACTIONS, Produced.with(Serdes.Long(), bankTransactionJsonSerde));

        return streamsBuilder.build();
    }
}
