import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "java-consumer");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("results"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(20));
            for (ConsumerRecord<String, String> record : records) {
                long localTimestamp = System.currentTimeMillis();
                String value = record.value();
                JSONObject jsonValue = new JSONObject(value);

                long window_end_timestamp = jsonValue.getLong("end_event_time");
                //long latency = record.timestamp() - window_end_timestamp;
                long window_size = jsonValue.getLong("window_size");
                long last_event_ts = jsonValue.getLong("last_event_ts");
                long latency = record.timestamp() - last_event_ts;

                String template = "%d; %s; %s; %d; %s; %d; %d; %d; %d;%n";
                String csv_out = String.format(template, record.offset(), record.key(), record.value(),
                        record.timestamp(), record.timestampType(), latency, localTimestamp, window_size, last_event_ts);
                try {
                    Files.write(Paths.get("resultsoutput.csv"), csv_out.getBytes(), StandardOpenOption.APPEND);
                } catch (IOException e) {
                    // TODO
                }
            }
        }
    }
}
