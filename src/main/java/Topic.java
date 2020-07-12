import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.common.config.TopicConfig.*;

public class Topic {

    private static void createTopic(AdminClient admin, String topic_name) throws ExecutionException, InterruptedException {
        NewTopic topic = new NewTopic(topic_name, 1, (short) 1);
        // TODO il numero di repliche non può essere maggiore al numero di broker altrimenti non lo crea
        Map<String, String> configs = new HashMap<>();
        configs.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
        configs.put(SEGMENT_MS_CONFIG, "1");
        configs.put(DELETE_RETENTION_MS_CONFIG, "0");
        configs.put(MAX_COMPACTION_LAG_MS_CONFIG, "1");
        configs.put(MIN_COMPACTION_LAG_MS_CONFIG, "0");
        topic.configs(configs);
        CreateTopicsResult result = admin.createTopics(Collections.singleton(topic));

        Collection<ConfigResource> cr =
                Collections.singleton( new ConfigResource(ConfigResource.Type.TOPIC, topic_name));
        DescribeConfigsResult ConfigsResult = admin.describeConfigs(cr);
        Config all_configs = (Config)ConfigsResult.all().get().values().toArray()[0];

        Iterator ConfigIterator = all_configs.entries().iterator();

        while (ConfigIterator.hasNext())
        {
            ConfigEntry currentConfig = (ConfigEntry) ConfigIterator.next();
            if (currentConfig.name().equals(CLEANUP_POLICY_CONFIG)) {
                System.out.println(currentConfig.value());
            }
        }
    }


    private static void lista(AdminClient admin){
        try {
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(true); // includes internal topics such as __consumer_offsets
            ListTopicsResult topics = admin.listTopics(options);
            Set<String> currentTopicList = topics.names().get();
            System.out.println(currentTopicList);
            // do your filter logic here......
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        AdminClient admin;  // TODO guarda se puoi toglierlo
        Properties props_admin = new Properties();
        props_admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        admin = AdminClient.create(props_admin);

        lista(admin);

        createTopic(admin, "__state_1.1.0.1");
        createTopic(admin, "__state_1.2.0.1");
        createTopic(admin, "__state_1.3.0.1");
        createTopic(admin, "__state_2.1.0.1");
        createTopic(admin, "__state_3.1.0.1");

        lista(admin);



    }
}
