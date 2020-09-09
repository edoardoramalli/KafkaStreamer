package stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

import org.apache.commons.cli.*;


import java.lang.Thread;


public class Streamer implements Runnable {

    private BiFunction<Integer, Integer, Integer> function;
    private String function_name;

    private int streamer_id;
    private String group_id = "0";
    private int node_id;
    private String transaction_id;

    private int input_stage;
    private int output_stage;
    private String topic_state;

    private String input_topic;
    private String output_topic;
    private boolean last_streamer = false;

    private boolean running = true;
    private boolean verbose = true;

    private final String bootstrap_server;

    private String name;

    private Properties props_producer;
    private Properties props_consumer;

    // Consumer & Producer
    private KafkaConsumer<String, String> consumer = null;
    private KafkaConsumer<String, String> consumer_state = null;
    private KafkaProducer<String, String> producer = null;

    // State
    private Map<String, String> state = new HashMap<>();
    private Map<String, String> old_state = new HashMap<>();
    private int num_msg = 0;

    // General Variable
    private final Duration poll_duration = Duration.of(10, ChronoUnit.SECONDS);


    public Streamer(String function, int streamer_id, int input_stage, int output_stage, int node_id,
                    String bootstrap_address) {

        this.parse_function(function);
        this.streamer_id = streamer_id;
        this.input_stage = input_stage;
        this.output_stage = output_stage;
        this.node_id = node_id;
        this.bootstrap_server = bootstrap_address;

        // Object variable

        this.name = this.streamer_id + "_" + this.input_stage + "_" + this.node_id;
        this.transaction_id = "__transaction_id_" + this.name;

        this.topic_state = "__state_" + this.name;
        this.input_topic = "__stage_" + this.streamer_id + "_" + this.input_stage;
        if (this.output_stage == -1) {
            this.last_streamer = true;
            this.output_topic = "__stage_" + this.streamer_id + "_END";
        } else {
            this.output_topic = "__stage_" + this.streamer_id + "_" + this.output_stage;
        }

        this.print_info();

        // Producer Properties
        this.props_producer = new Properties();
        this.props_producer.put("bootstrap.servers", this.bootstrap_server);
        this.props_producer.put("key.serializer", StringSerializer.class.getName());
        this.props_producer.put("value.serializer", StringSerializer.class.getName());
        this.props_producer.put("transactional.id", this.transaction_id);
        this.props_producer.put("enable.idempotence", "true");
        this.props_producer.put("retries", "3");
        this.props_producer.put("acks", "all");
        this.props_producer.put("transaction.timeout.ms", "20000");

        this.setUpProducer();

        // Consumer Properties
        this.props_consumer = new Properties();
        this.props_consumer.put("bootstrap.servers", this.bootstrap_server);
        this.props_consumer.put("group.id", this.group_id);
        this.props_consumer.put("key.deserializer", StringDeserializer.class.getName());
        this.props_consumer.put("value.deserializer", StringDeserializer.class.getName());
        this.props_consumer.put("isolation.level", "read_committed");
        this.props_consumer.put("enable.auto.commit", "false");
        this.props_consumer.put("auto.offset.reset", "earliest");

        // Resume previous state if present
        this.resume_state();

        this.setUpConsumer();
    }

    /**
     * Initialize consumer state and subscribe to state topic
     */
    private void setUpConsumerState() {
        Properties p = new Properties();
        p.put("bootstrap.servers", this.bootstrap_server);
        p.put("group.id", this.group_id);
        p.put("key.deserializer", StringDeserializer.class.getName());
        p.put("value.deserializer", StringDeserializer.class.getName());
        p.put("enable.auto.commit", "false");
        p.put("isolation.level", "read_committed");
        p.put("auto.offset.reset", "earliest");


        this.consumer_state = new KafkaConsumer<>(p);
        final List<String> topics_subscription = new ArrayList<>();
        topics_subscription.add(this.topic_state);
        this.consumer_state.subscribe(topics_subscription);
    }

    /**
     * Initialize Consumer and subscribe to input topic
     */
    private void setUpConsumer() {
        this.consumer = new KafkaConsumer<>(this.props_consumer);
        final List<String> topics_subscription = new ArrayList<>();
        topics_subscription.add(this.input_topic);
        this.consumer.subscribe(topics_subscription);
    }

    /**
     * Initialize Producer
     */
    private void setUpProducer() {
        this.producer = new KafkaProducer<>(this.props_producer);
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

    /**
     * Transform from string name to pointer to the function
     * @param function_name name of the operation
     */
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

    /**
     * Poll messages from topic state
     * Compute new state
     * Commit message
     * Until the poll is empty. Ensure that we have consumed all the messages in topic state
     */
    private void get_state() {
        boolean empty = false;
        while(!empty) {
            final ConsumerRecords<String, String> records = this.consumer_state.poll(this.poll_duration);
            for (final ConsumerRecord<String, String> record : records) {
                this.start_transaction();
                this.compute_state(record);
                this.commit_transaction(record);
            }
            if(records.count() == 0){
                empty = true;
            }
        }
        if (this.verbose) {
            System.out.println("Streamer " + this.name + " - ID " +
                    this.streamer_id +
                    " - State  : " +
                    this.state);
        }
    }

    /**
     * Transform string record into dictionary state
     * @param record of the state
     */
    private void compute_state(ConsumerRecord<String, String> record) {
        if (record.key().equals("state")) {
            this.state = Streamer.stringToMap(record.value());
        } else {
            System.err.println("Not Expected Key");
        }
    }

    /**
     * Rollback to previous state if transaction fail
     */
    private void rollback_state(){
        this.state = this.old_state;
        if (this.verbose) {
            System.out.println("Streamer " + this.name + " - ID " +
                    this.streamer_id +
                    " - State  : " +
                    this.state);
        }
    }

    /**
     * Update the state based on the key of current record.
     * The state keeps the count of how many record with that key are processed.
     * @param record under processing
     */
    private void update_state(ConsumerRecord<String, String> record) {
        this.old_state = this.state;
        if (this.state.containsKey(record.key())) {
            this.state.put(record.key(), String.valueOf(Integer.parseInt(this.state.get(record.key())) + 1));
        } else {
            this.state.put(record.key(), String.valueOf(1));
        }
        if (this.verbose) {
            System.out.println("Streamer " + this.name + " - ID " +
                    this.streamer_id +
                    " - State  : " +
                    this.state);
        }
        final ProducerRecord<String, String> produce_record =
                new ProducerRecord<>(this.topic_state, "state", Streamer.mapToString(this.state));
        final Future<RecordMetadata> future = this.producer.send(produce_record);
        this.producer.flush();

    }

    /**
     * Get a poll of records from the input topic
     */
    private ConsumerRecords<String, String> getPoll() {
       return this.consumer.poll(this.poll_duration);
    }

    private void consume(ConsumerRecord<String, String> record){
        if (this.verbose) {
            if (!record.key().equals("state")) {
                System.out.println("Streamer " + this.name + " - ID " +
                        this.streamer_id +
                        " - Consume: " +
                        record.key() +
                        " = " +
                        record.value() +
                        " - Partition: " +
                        record.partition());
            } else {
                System.out.println("Streamer " + this.name + " - ID " +
                        this.streamer_id +
                        " - Consume: State" +
                        " - Partition: " +
                        record.partition());
            }
        }
    }


    /**
     * Given an input record
     * Update number of messages
     * Update the current state in a safe mode
     * Apply the function of the stage (second operator is always 1)
     * Produce a new record with new value but keeping same key
     * @param record under processing
     * @return record produced for downstream
     */
    private ConsumerRecord<String, String> compute(ConsumerRecord<String, String> record) {

        this.num_msg += 1;

        this.update_state(record);

        int new_value = this.function.apply(Integer.parseInt(record.value()), 1);

        ConsumerRecord<String, String> new_record =
                new ConsumerRecord<>
                        (this.output_topic,
                                -1,
                                -1,
                                record.key(),
                                String.valueOf(new_value));

        if (this.verbose) {
            System.out.println("Streamer " + this.name + " - ID " +
                    this.streamer_id +
                    " - Process: " +
                    record.key() +
                    " = " +
                    record.value() +
                    " => " +
                    this.function_name +
                    " State " + this.state.get(record.key()) +
                    " => " +
                    new_record.key() +
                    " = " +
                    new_record.value() +
                    " - Msg. " +
                    this.num_msg);
        }
        return new_record;
    }

    /**
     * If is not the last stage of pipeline, produce downstream record
     * @param record new record to produce for downstream
     */
    private void produce(ConsumerRecord<String, String> record) {
        if (!this.last_streamer) {
            final ProducerRecord<String, String> produce_record =
                    new ProducerRecord<>(this.output_topic, record.key(), record.value());
            final Future<RecordMetadata> future = producer.send(produce_record);
            if (this.verbose) {
                System.out.println("Streamer " + this.name + " - ID " +
                        this.streamer_id +
                        " - Produce: " +
                        record.key() +
                        " = " +
                        record.value());
            }
        } else {
            if (this.verbose) {
                System.out.println("Streamer " + this.name + " - ID " +
                        this.streamer_id +
                        " - Close  : " +
                        record.key() +
                        " = " +
                        record.value());
            }
        }

    }

    /**
     * Producer commit offsets consumer and writes in an atomic way
     * Otherwise consumer and producer don't know each other
     * @param record under processing
     */
    private void commit_transaction(ConsumerRecord<String, String> record) {
        final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        map.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
        this.producer.sendOffsetsToTransaction(map, this.group_id);
        this.producer.commitTransaction();
        if (this.verbose && !record.key().equals("state")) {
            System.out.println("Streamer " + this.name + " - ID " +
                    this.streamer_id +
                    " - Commit : " +
                    record.key() +
                    " = " +
                    record.value() +
                    " - Partition: " +
                    record.partition());
        }
        if (this.verbose && record.key().equals("state")) {
            System.out.println("Streamer " + this.name + " - ID " +
                    this.streamer_id +
                    " - Commit : State" +
                    " - Partition: " +
                    record.partition());
        }
    }

    /**
     * Begin Transaction
     */
    private void start_transaction() {
        this.producer.beginTransaction();
    }

    private void print_info() {
        System.out.println("===INFO===");
        System.out.println("BootStrap Server:  " + this.bootstrap_server);
        System.out.println("Name Streamer:  " + this.name);
        System.out.println("Input Topic:  " + this.input_topic);
        System.out.println("Output Topic:  " + this.output_topic);
        System.out.println("State Topic:  " + this.topic_state);
        System.out.println("==========");
    }

    /**
     * Subscribe consumer to state topic
     * Poll old state if present
     * Update state
     * Unsubscribe from state topic
     * close consumer state
     */
    private void resume_state() {
        this.setUpConsumerState();
        this.get_state();
        this.consumer_state.close();
    }


    /**
     * Consume a poll of record
     * Start Transaction
     * Process one record
     * Compute
     * Update State
     * Register new state
     * Produce one record for downstream if present
     * Commit Transaction on processed one record
     */
    @Override
    public void run() {
        try {
            while (this.running) {
                try {
                    for (final ConsumerRecord<String, String> record : this.getPoll()) {
                        System.out.println();
                        this.consume(record);
                        this.start_transaction();
                        ConsumerRecord<String, String> new_record = this.compute(record);
                        this.produce(new_record);
                        this.commit_transaction(record);
                        Thread.sleep(1000);
                    }
                } catch (final KafkaException e) {
                    this.producer.flush();
                    this.producer.abortTransaction();
                    this.rollback_state();
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            if (!this.last_streamer) {
                this.producer.close();
            }
        }
    }


    public static void main(String[] args){

        CommandLine cmd = parseArguments(args);

        int input_stage = Integer.parseInt(cmd.getOptionValue("input"));
        String out_parse = cmd.getOptionValue("output");
        int output_stage = -1;
        if (out_parse != null) {
            output_stage = Integer.parseInt(cmd.getOptionValue("output"));
        }
        int stream_id = Integer.parseInt(cmd.getOptionValues("stream")[0]);
        int node_id = Integer.parseInt(cmd.getOptionValues("node")[0]);
        String bootstrap_address = String.valueOf(cmd.getOptionValues("bootstrap")[0]);
        String operation = cmd.getOptionValue("function");

        Streamer stream = new Streamer(operation, stream_id, input_stage, output_stage, node_id, bootstrap_address);

        stream.run();

    }

    private static CommandLine parseArguments(String[] args){
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
        output.setRequired(false);
        options.addOption(output);

        Option name = new Option("n", "node", true, "Node ID");
        output.setRequired(true);
        options.addOption(name);

        Option bootstrap = new Option("b", "bootstrap", true, "Boostrap ID and port");
        output.setRequired(true);
        options.addOption(bootstrap);

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

        return cmd;
    }

    private static String mapToString(Map<String, String> map) {
        StringBuilder stringBuilder = new StringBuilder();

        for (String key : map.keySet()) {
            if (stringBuilder.length() > 0) {
                stringBuilder.append("&");
            }
            String value = map.get(key);
            try {
                stringBuilder.append((key != null ? URLEncoder.encode(key, "UTF-8") : ""));
                stringBuilder.append("=");
                stringBuilder.append(value != null ? URLEncoder.encode(value, "UTF-8") : "");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("This method requires UTF-8 encoding support", e);
            }
        }

        return stringBuilder.toString();
    }

    private static Map<String, String> stringToMap(String input) {
        Map<String, String> map = new HashMap<>();

        if (input.equals("")) {
            return map;
        }

        String[] nameValuePairs = input.split("&");
        for (String nameValuePair : nameValuePairs) {
            String[] nameValue = nameValuePair.split("=");
            try {
                map.put(URLDecoder.decode(nameValue[0], "UTF-8"), nameValue.length > 1 ? URLDecoder.decode(
                        nameValue[1], "UTF-8") : "");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("This method requires UTF-8 encoding support", e);
            }
        }

        return map;
    }


}


