import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

import org.apache.commons.cli.*;


public class Streamer implements Runnable {

    private BiFunction<Integer, Integer, Integer> function;
    private String function_name;


    private int streamer_id;
    private int input_stage;
    private int output_stage;
    private String input_topic = "";
    private String output_topic = "";
    private boolean last_streamer = false;
    private boolean first_streamer = false;

    private boolean running = true;
    private boolean verbose = true;

    private String group_id = "0";

    private int node_id;
    private String transaction_id;

    // Consumer
    private ConsumerRecords<String, String> last_record_read;

    // Consumer & Producer
    private KafkaConsumer<String, String> consumer = null;
    private KafkaProducer<String, String> producer = null;

    // State
    private Map<String, Integer> dictionary = new HashMap<>();
    private int num_msg = 0;
    final Random generator = new Random();


    public Streamer(String function, int streamer_id, int input_stage, int output_stage, int node_id) {
        this.parse_function(function);
        this.streamer_id = streamer_id;
        this.input_stage = input_stage;
        this.output_stage = output_stage;
        this.node_id = node_id;

        this.transaction_id = "transaction_id_" +
                String.valueOf(this.streamer_id) + "_" +
                String.valueOf(this.input_stage) + "_" +
                String.valueOf(this.output_stage) + "_" +
                String.valueOf(this.node_id);



        if (this.input_stage == -1) {
            this.first_streamer = true;
        } else {
            this.input_topic = String.valueOf(this.streamer_id) + "_" + String.valueOf(this.input_stage);
        }


        if (this.output_stage == -1) {
            this.last_streamer = true;
        } else {
            if (!this.first_streamer) {
                this.output_topic = String.valueOf(this.streamer_id) + "_" + String.valueOf(this.output_stage);
            } else {
                this.output_topic = String.valueOf(this.streamer_id) + "_" + String.valueOf(0);
            }

        }

//        // Admin Client and get Topics
//        Properties config = new Properties();
//        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        AdminClient admin = AdminClient.create(config); // TODO guarda se puoi toglierlo
//
//        // Create DownStream Topic
//        if (!this.last_streamer) {
//            int num_partitions = 5;
//            short num_replicas = (short) 1;
//            NewTopic newTopic = new NewTopic(this.output_topic, num_partitions, num_replicas);
//            admin.createTopics(Collections.singleton(newTopic));
//            this.setUpProducer();
//        }
//
//        // Check UpStream Topic
//        Set<String> topics = admin.listTopics().names().get();
//        if (!topics.contains(this.input_topic)) {
//            System.err.println("Error: No UpStream Topic");
//            System.exit(1);
//        }
//
//        admin.close();

        this.setUpProducer();

        this.setUpConsumer();


    }

    public Streamer(String function, int streamer_id, int input_stage, int node_id) {
        this(function, streamer_id, input_stage, -1, node_id);
    }

    private void setUpConsumer() {
        // Consumer SetUp
        final Properties props_consumer = new Properties();
        props_consumer.put("bootstrap.servers", "localhost:9092");
        props_consumer.put("group.id", this.group_id); // TODO deal with group
//        props_consumer.put("auto.commit.interval.ms", "10000");

//        //  TODO decidere su questi
//        props_consumer.put("max.poll.records", "1");
//        props_consumer.put("max.poll.interval.ms", "500");

        props_consumer.put("isolation.level", "read_committed");
        props_consumer.put("enable.auto.commit", "false");

        // default: latest, try earliest
        props_consumer.put("key.deserializer", StringDeserializer.class.getName());
        props_consumer.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props_consumer);
        final List<String> topics_subscription = new ArrayList<>();
        topics_subscription.add(this.input_topic);
        consumer.subscribe(topics_subscription);

    }

    private void setUpProducer() {
        final Properties props_producer = new Properties();
        props_producer.put("bootstrap.servers", "localhost:9092");
        props_producer.put("key.serializer", StringSerializer.class.getName());
        props_producer.put("value.serializer", StringSerializer.class.getName());
        props_producer.put("transactional.id", this.transaction_id);
        props_producer.put("enable.idempotence", "true");
        props_producer.put("retries", "3");
        props_producer.put("acks", "all");
        this.producer = new KafkaProducer<>(props_producer);
        producer.initTransactions();
    }

    private static int adder(int input, int state) {
        return input + state;
    }

    private static int power(int input, int state) {
        return (int) Math.pow(input, state);
    }

    private static int diff(int input, int state) {
        return input - state;
    }

    private static int identity(int input, int state) {
        return input;
    }

    private void parse_function(String function_name) {
        switch (function_name) {
            case "adder":
                this.function = Streamer::adder;
                this.function_name = "adder";
                break;
            case "power":
                this.function = Streamer::power;
                this.function_name = "power";
                break;
            case "diff":
                this.function = Streamer::diff;
                this.function_name = "diff";
                break;
            default:
                this.function = Streamer::identity;
                this.function_name = "identity";
                break;
        }
    }

    private void consume() {
        this.last_record_read = null;
        if (!this.first_streamer) {
            this.last_record_read = this.consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
            if (this.verbose) {
                for (final ConsumerRecord<String, String> record : this.last_record_read) {
                    System.out.println("Streamer ID " +
                            this.streamer_id +
                            " - Consume: " +
                            record.key() +
                            " = " +
                            record.value() +
                            " - Partition: " +
                            record.partition());
                }
            }
        }
//        else {
//            String record_key = "Key" + this.generator.nextInt(30);
//            String record_value = String.valueOf(this.generator.nextInt(300));
//            ConsumerRecord<String, String> new_record =
//                    new ConsumerRecord<>
//                            (this.output_topic, -1, -1, record_key, record_value);
//            List<ConsumerRecord<String, String>> record_list = new ArrayList<>();
//            record_list.add(new_record);
//            this.last_record_read = record_list;
//
//            if (this.verbose) {
//                System.out.println("Streamer ID " +
//                        this.streamer_id +
//                        " - Create : " +
//                        new_record.key() +
//                        " = " +
//                        new_record.value());
//            }
//        }

    }

    private ConsumerRecord<String, String> compute(ConsumerRecord<String, String> record) {

        this.num_msg += 1;

//            int new_value = this.function.apply(
//                    Integer.parseInt(consume_record.value()),
//                    this.dictionary.get(consume_record.key()));

        int new_value = this.function.apply(
                Integer.parseInt(record.value()), 2); // TODO Fix this


        ConsumerRecord<String, String> new_record =
                new ConsumerRecord<>
                        (this.output_topic,
                                record.partition(),
                                record.offset(),
                                record.key(),
                                String.valueOf(new_value));

        if (this.verbose) {
            System.out.println("Streamer ID " +
                    this.streamer_id +
                    " - Process: " +
                    record.key() +
                    " = " +
                    record.value() +
                    " => " +
                    this.function_name +
                    " => " +
                    new_record.key() +
                    " = " +
                    new_record.value() +
                    " - Msg. " +
                    this.num_msg);
        }

        return new_record;
    }

    private void produce(ConsumerRecord<String, String> record) {
        if (!this.last_streamer) {

            final ProducerRecord<String, String> produce_record =
                    new ProducerRecord<>(this.output_topic, record.key(), record.value());
            final Future<RecordMetadata> future = producer.send(produce_record);

            boolean waitAck = false; //TODO capire se serve veramente

            if (waitAck) {
                try {
                    System.out.println(future.get());
                } catch (InterruptedException | ExecutionException e1) {
                    e1.printStackTrace();
                }
            }

            if (this.verbose) {
                System.out.println("Streamer ID " +
                        this.streamer_id +
                        " - Produce: " +
                        record.key() +
                        " = " +
                        record.value());
            }

        }
        else{
            if (this.verbose) {
                System.out.println("Streamer ID " +
                        this.streamer_id +
                        " - Close: " +
                        record.key() +
                        " = " +
                        record.value());
            }
        }

    }

    private void commit_transaction(ConsumerRecord<String, String> record){
        final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        map.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
        this.producer.sendOffsetsToTransaction(map, this.group_id);
        this.producer.commitTransaction();
        if (this.verbose){
            System.out.println("Streamer ID " +
                    this.streamer_id +
                    " - Commit : " +
                    record.key() +
                    " = " +
                    record.value() +
                    " - Partition: " +
                    record.partition());
        }
    }

    private void start_transaction(){
        this.producer.beginTransaction();
    }


    @Override
    public void run() {
        try {
            while (this.running) {


                try {
                    this.consume();
                    for (final ConsumerRecord<String, String> record : this.last_record_read) {
                        this.start_transaction();
                        ConsumerRecord<String, String> new_record = this.compute(record);
                        this.produce(new_record);
                        this.commit_transaction(record);
                    }

                }  catch (final KafkaException e) {
                    e.printStackTrace();
                    // For all other exceptions, just abort the transaction and try again.
                    this.producer.abortTransaction();
                }

                // Sleep

                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }

            }
        } finally {
            if (!this.first_streamer){
                this.consumer.close();
            }
            if  (!this.last_streamer){
                this.producer.close();
            }
        }
    }



    public static void main(String[] args) throws ExecutionException, InterruptedException {



        Options options = new Options();

        Option function = new Option("f", "function", true, "Function of the streamer");
        function.setRequired(true);
        options.addOption(function);

        Option stream = new Option("s", "stream", true, "Streamer ID");
        stream.setRequired(true);
        options.addOption(stream);

        Option input = new Option("i", "input", true, "Input Stage");
        input.setRequired(true);
        options.addOption(input);

        Option output = new Option("o", "output", true, "Output Stage");
        output.setRequired(true);
        options.addOption(output);

        Option name = new Option("n", "node", true, "Node ID");
        output.setRequired(true);
        options.addOption(name);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }

        int input_stage = Integer.parseInt(cmd.getOptionValue("input"));
        int output_stage = Integer.parseInt(cmd.getOptionValue("output"));
        int stream_id = Integer.parseInt(cmd.getOptionValues("stream")[0]);
        int node_id = Integer.parseInt(cmd.getOptionValues("node")[0]);

        String operation = cmd.getOptionValue("function");


        Streamer edo = new Streamer(operation, stream_id, input_stage, output_stage, node_id);



        edo.run();

    }


}
