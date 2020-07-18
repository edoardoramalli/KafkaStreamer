package stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import org.apache.commons.cli.*;

public class Producer {

    public static void main(String[] args) throws InterruptedException {
        final int numMessages = 100000;

        Options options = new Options();

        Option function = new Option("w", "wait", true, "Wait between messages");
        function.setRequired(true);
        options.addOption(function);

        Option stream = new Option("s", "stream", true, "Streamer ID");
        stream.setRequired(true);
        options.addOption(stream);

        Option output = new Option("o", "output", true, "Output Stage");
        output.setRequired(false);
        options.addOption(output);

        Option bootstrap = new Option("b", "bootstrap", true, "Boostrap ID and port");
        output.setRequired(true);
        options.addOption(bootstrap);

        Option part = new Option("p", "partition", true, "Number of partition");
        output.setRequired(true);
        options.addOption(part);

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

        int waitBetweenMsgs = Integer.parseInt(cmd.getOptionValue("wait"));
        int output_stage = Integer.parseInt(cmd.getOptionValue("output"));
        int stream_id = Integer.parseInt(cmd.getOptionValues("stream")[0]);
        int partition = Integer.parseInt(cmd.getOptionValues("partition")[0]);
        String bootstrap_address = String.valueOf(cmd.getOptionValues("bootstrap")[0]);


        boolean print = true;

        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap_address);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("enable.idempotence", true);
        props.put("transactional.id", "producer");

        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        producer.initTransactions();


        for (int i = 0; i < numMessages; i++) {
            final String topic = "__stage_" + stream_id + "_" + output_stage;
            final String key = "Key" + String.valueOf(i % partition);
            final String value = String.valueOf(i);
            if (print) {
                System.out.println("Topic: " + topic + "\t" + //
                        "Key: " + key + "\t" + //
                        "Value: " + value);
            }
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, key, value));
            producer.commitTransaction();

            try {
                Thread.sleep(waitBetweenMsgs);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();

    }
}
