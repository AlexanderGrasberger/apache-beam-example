package apache.beam.sample.stream;

import dnl.utils.text.table.TextTable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;

public class CustomKafkaConsumer implements Runnable{

    private HashMap<String, Long> wordCount;
    private Consumer<String, Long> consumer;

    public CustomKafkaConsumer(Consumer<String, Long> consumer) {
        this.consumer = consumer;
        this.wordCount = new HashMap<>();
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList("word_count_topic"));
        while (!Thread.currentThread().isInterrupted()) {
            ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, Long> record : records) {
                Long newValue = this.wordCount.getOrDefault(record.key(), 0L) + record.value();
                this.wordCount.put(record.key(), newValue);
                System.out.println("\u001B[34mRecieved from Kafka: { Key: " + record.key() + " Value: " + record.value() + "}\u001B[30m");
                printTable();
            }
        }
    }

    private void printTable() {
        Object[][] data = new Object[this.wordCount.size()][2];

        int index = 0;
        for (HashMap.Entry<String, Long> entry : this.wordCount.entrySet()) {
            data[index][0] = entry.getKey();
            data[index][1] = entry.getValue();
            index++;
        }

        TextTable table = new TextTable(new String[] {"Wort", "Anzahl"}, data);
        table.printTable();
    }

}
