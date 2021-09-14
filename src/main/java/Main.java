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
    public static void main(String[] args){
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
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                // TODO: aus value timestamp auslesen und latency calculaten
                // TODO: mit "" im korrekten jsonformat ausgeben in kafka emitter in storm topology
                //String value  = record.value();
                //JSONObject jsonValue = new JSONObject(value);
                //long window_end_timestamp = jsonValue.getLong("end_event_time");
                //long latency = record.timestamp() - window_end_timestamp

                String template = "%d; %s; %s; %d; %s%n";
                String csv_out = String.format(template, record.offset(), record.key(), record.value(), record.timestamp(), record.timestampType() );
                try {
                    Files.write(Paths.get("resultsoutput.csv"),csv_out.getBytes(), StandardOpenOption.APPEND);
                } catch (IOException e) {
                    //exception handling left as an exercise for the reader
                }
                //System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
