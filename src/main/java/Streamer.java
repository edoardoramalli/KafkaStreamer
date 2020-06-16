import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiFunction;


public class Streamer implements Runnable {

    private BiFunction<Integer, Integer, Integer> function;
    private int streamer_id;
    private int input_stage;
    private int output_stage;
    private String input_topic;
    private String output_topic;
    private boolean last_streamer;

    private boolean running = true;
    private boolean verbose = true;

    // Consumer
    private KafkaConsumer<String, String> consumer = null;
    private KafkaProducer<String, String> producer = null;

    // State
    private Map<String, Integer> dictionary = new HashMap<>();
    private int num_msg = 0;


    public Streamer(String function, int streamer_id, int input_stage, int output_stage)
            throws ExecutionException, InterruptedException {
        this.parse_function(function);
        this.streamer_id = streamer_id;
        this.input_stage = input_stage;
        this.output_stage = output_stage;

        this.input_topic = String.valueOf(this.streamer_id) + "_" + String.valueOf(this.input_stage);

        if (this.output_stage == -1) {
            this.last_streamer = true;
        } else {
            this.output_topic = String.valueOf(this.streamer_id) + "_" + String.valueOf(this.output_stage);
        }

        // Admin Client and get Topics
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(config); // TODO guarda se puoi toglierlo

        // Create DownStream Topic
        if (!this.last_streamer) {
            int num_partitions = 5;
            short num_replicas = (short) 1;
            NewTopic newTopic = new NewTopic(this.output_topic, num_partitions, num_replicas);
            admin.createTopics(Collections.singleton(newTopic));
            this.setUpProducer();
        }

        // Check UpStream Topic
        Set<String> topics = admin.listTopics().names().get();
        if (!topics.contains(this.input_topic)) {
            System.err.println("Error: No UpStream Topic");
            System.exit(1);
        }

        admin.close();

        this.setUpConsumer();


    }

    public Streamer(String function, int streamer_id, int input_stage)
            throws ExecutionException, InterruptedException {
        this(function, streamer_id, input_stage, -1);
    }

    private void setUpConsumer(){
        // Consumer SetUp
        final Properties props_consumer = new Properties();
        props_consumer.put("bootstrap.servers","localhost:9092");
        props_consumer.put("group.id", "0"); // TODO deal with group
        props_consumer.put("enable.auto.commit","true"); // Default value
        props_consumer.put("auto.commit.interval.ms","10000");

        // default: latest, try earliest
        props_consumer.put("auto.offset.reset","earliest");
        props_consumer.put("key.deserializer",StringDeserializer.class.getName());
        props_consumer.put("value.deserializer",StringDeserializer .class.getName());
        this.consumer = new KafkaConsumer<>(props_consumer);
        final List<String> topics_subscription = new ArrayList<>();
        topics_subscription.add(this.input_topic);
        consumer.subscribe(topics_subscription);

    }

    private void setUpProducer(){
        boolean idempotent = false;
        final Properties props_producer = new Properties();
        props_producer.put("bootstrap.servers", "localhost:9092");
        props_producer.put("key.serializer", StringSerializer.class.getName());
        props_producer.put("value.serializer", StringSerializer.class.getName());
        props_producer.put("enable.idempotence", idempotent);
        this.producer = new KafkaProducer<>(props_producer);
    }

    private static int adder(int input, int state){
        return input + state;
    }

    private static int power(int input, int state){
        return (int) Math.pow(input, state);
    }

    private static int diff(int input, int state){
        return input - state;
    }

    private static int identity(int input, int state){
        return input;
    }

    private void parse_function(String function_name){
        switch (function_name){
            case "adder":
                this.function = Streamer::adder;
                break;
            case "power":
                this.function = Streamer::power;
                break;
            case "diff":
                this.function = Streamer::diff;
                break;
            default:
                this.function = Streamer::identity;
                break;
        }
    }


    @Override
    public void run() {
        try {
            while (this.running) {
                // Consume
                final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, String> consume_record : records) {
                    this.num_msg += 1;
                    String current_key = consume_record.key();
                    if (this.dictionary.containsKey(current_key)){
                        this.dictionary.put(current_key, this.dictionary.get(current_key) + 1);
                        // TODO controlla se crea un item nuovo o lo aggiorna
                    }
                    else{
                        this.dictionary.put(current_key, 1);
                    }

                    // Compute
                    int current_state = this.function.apply(
                            Integer.parseInt(consume_record.value()),
                            this.dictionary.get(current_key));

                    // Produce
                    if (!this.last_streamer) {
                        final String key = consume_record.key();
                        final String value = String.valueOf(current_state);
                        final ProducerRecord<String, String> produce_record =
                                new ProducerRecord<>(this.output_topic, key, value);
                        final Future<RecordMetadata> future = producer.send(produce_record);

                        boolean waitAck = false;
                        if (waitAck) {
                            try {
                                System.out.println(future.get());
                            } catch (InterruptedException | ExecutionException e1) {
                                e1.printStackTrace();
                            }
                        }
                        if(this.verbose) {
                            System.out.print("\rStreamer - # " +
                                    this.num_msg  +
                                    " - Key: " + consume_record.key()
                                    + " | [" + this.input_topic + "] " +
                                    consume_record.value() +
                                    " --> " +
                                    current_state +
                                    " --> [" + this.output_topic + "]");
                        }
                    }
                    else{
                        if(this.verbose) {
                            System.out.print("\rStreamer - # " +
                                    this.num_msg  +
                                    " - Key: " + consume_record.key()
                                    + " | [" + this.input_topic + "] " +
                                    consume_record.value() +
                                    " --> " +
                                    current_state +
                                    " --> [end]");
                        }
                    }
                }


                // Sleep


                try {
                    Thread.sleep(5000);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }

            }
        } finally {
            this.consumer.close();
            this.producer.close();
        }
    }




    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Streamer edo = new Streamer("add", 1, 2, 3);
        edo.run();

    }
}
