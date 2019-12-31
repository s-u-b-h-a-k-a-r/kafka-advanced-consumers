package com.kafka.learning.kafka.consumer;

import static com.kafka.learning.kafka.consumer.DatabaseUtilities.getConnection;
import static com.kafka.learning.kafka.consumer.DatabaseUtilities.initDB;
import static com.kafka.learning.kafka.consumer.DatabaseUtilities.readDB;
import static com.kafka.learning.kafka.consumer.DatabaseUtilities.saveStockPrice;
import static com.kafka.learning.kafka.consumer.DatabaseUtilities.startJdbcTransaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.learning.kafka.StockAppConstants;
import com.kafka.learning.producer.model.StockPrice;

public class SimpleStockPriceConsumer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleStockPriceConsumer.class);

    private static Consumer<String, StockPrice> createConsumer() {
        final Properties props = new Properties();

        // Turn off auto commit - "enable.auto.commit".
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, StockAppConstants.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Custom Deserializer
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StockDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        // Create the consumer using props.
        final Consumer<String, StockPrice> consumer = new KafkaConsumer<>(props);

        return consumer;
    }

    static void runConsumer(final int readCountStatusUpdate) throws Exception {

        // Create consumer
        final Consumer<String, StockPrice> consumer = createConsumer();
        final boolean running = true;

        consumer.subscribe(Collections.singletonList(StockAppConstants.TOPIC), new SeekToLatestRecordsConsumerRebalanceListener(consumer));

        final Map<String, StockPriceRecord> lastRecordPerStock = new HashMap<>();

        // Initialize current records with what is in DB.
        readDB().forEach(stockPriceRecord -> {
            lastRecordPerStock.put(stockPriceRecord.getName(), stockPriceRecord);
        });

        try {
            int readCount = 0;
            while (running) {
                pollRecordsAndProcess(readCountStatusUpdate, consumer, lastRecordPerStock, readCount);
            }
        } finally {
            consumer.close();
        }
    }

    private static void pollRecordsAndProcess(final int readCountStatusUpdate, final Consumer<String, StockPrice> consumer, final Map<String, StockPriceRecord> currentStocks, final int readCount) throws Exception {
        final ConsumerRecords<String, StockPrice> consumerRecords = consumer.poll(Duration.ofMillis(1000));

        if (consumerRecords.count() == 0)
            return;

        // Get rid of duplicates and keep only the latest record.
        consumerRecords.forEach(record -> currentStocks.put(record.key(), new StockPriceRecord(record.value(), false, record)));

        final Connection connection = getConnection();
        try {
            startJdbcTransaction(connection); // Start DB Transaction
            for (StockPriceRecord stockRecordPair : currentStocks.values()) {
                if (!stockRecordPair.isSaved()) {
                    // Save the record
                    // with partition/offset to DB.
                    saveStockPrice(stockRecordPair, connection);
                    // Mark the record as saved
                    currentStocks.put(stockRecordPair.getName(), new StockPriceRecord(stockRecordPair, true));
                }
            }
            consumer.commitSync(); // Commit the Kafka offset
            connection.commit(); // Commit DB Transaction
        } catch (CommitFailedException ex) {
            logger.error("Failed to commit sync to log", ex);
            connection.rollback(); // Rollback Transaction
        } catch (SQLException sqle) {
            logger.error("Failed to write to DB", sqle);
            connection.rollback(); // Rollback Transaction
        } finally {
            connection.close();
        }

        if (readCount % readCountStatusUpdate == 0) {
            displayRecordsStatsAndStocks(currentStocks, consumerRecords);
        }
    }

    private static void displayRecordsStatsAndStocks(final Map<String, StockPriceRecord> stockPriceMap, final ConsumerRecords<String, StockPrice> consumerRecords) {
        System.out.printf("New ConsumerRecords par count %d count %d, max offset %d\n", consumerRecords.partitions().size(), consumerRecords.count(), getHighestOffset(consumerRecords));
        stockPriceMap.forEach((s, stockPrice) -> System.out.printf("ticker %s price %d.%d saved %s\n", stockPrice.getName(), stockPrice.getDollars(), stockPrice.getCents(), stockPrice.isSaved()));
        System.out.println();
        System.out.println("Database Records");
        DatabaseUtilities.readDB()
                .forEach(stockPriceRecord -> System.out.printf("ticker %s price %d.%d saved from %s-%d-%d\n", stockPriceRecord.getName(), stockPriceRecord.getDollars(), stockPriceRecord.getCents(), stockPriceRecord.getTopic(), stockPriceRecord.getPartition(), stockPriceRecord.getOffset()));
        System.out.println();
    }

    private static long getHighestOffset(ConsumerRecords<String, StockPrice> consumerRecords) {
        long maxOffsetSeen = 0;
        for (ConsumerRecord<String, StockPrice> record : consumerRecords) {
            if (record.offset() > maxOffsetSeen) {
                maxOffsetSeen = record.offset();
            }
        }
        return maxOffsetSeen;
    }

    public static void main(String... args) throws Exception {

        initDB();

        runConsumer(10);
    }
}
