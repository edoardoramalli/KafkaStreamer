import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) throws InterruptedException {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "0");


        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        final List<String> topics_subscription = new ArrayList<>();
        topics_subscription.add("_state_0.1.1");

        consumer.subscribe(topics_subscription);

        final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
        for (final ConsumerRecord<String, String> record : records) {
            System.out.println(record.value());
        }
    }
}
