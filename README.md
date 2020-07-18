# KafkaStreamer
We provide administrative tools to create the bash files to lunch a kafka stream using as API, Consumer API and Producer API, on different processes. A stream is a sequence of stateful stages where in each of them a message is read from the previous output stage, is performed a basic operation, change the current state of the stage, and produce an output for the downstream stage.
Each message is a pair of `<key,value>`, and by design, is guaranteed that a message with the same key is processed along the same stream unless of rebalancing due to crash of a part's stage of the pipeline. In this way we respect the fifo order of the messages.
Using the Transaction method we can guarantee exactly one semantic.

## Usage

### 1. XML File
An XML file, for istance, `template.xml`,is used to define the structure of the stream. The minimal structure is the following one:
```xml
<?xml version="1.0"?>
<Stream id="id-number">
    <bootstrap value="ip-broker:port-broker"/>
    <zookeeper value="ip-zookeeper:port-zookeeper"/>
    <log value="path-to-log-folder"/>
    <kafka value="path-to-kafka-bin-folder"/>
    <replica value="number-replica"/>
    <partition value="number-partition"/>
    <jar value="path-to-stream-jar"/>
    <Streamer>
        <stage>stage-number</stage>
        <operation>operation</operation>
    </Streamer>
</Stream>
```
The mandatory parameters are:
- **id**: stream id
- **zookeeper**: ip and port of zookeeper
- **boostrap**: at least an ip and port of a kafka broker
- **log**: path to a custom log folder
- **kafka**: path to bin directory of kafka distribution
- **partition**: number of partition for the topics
- **replica**: number of replica to be fault tollerant. Same number of boostrap elements

### 2. Bash File
Using the command:
`python3 KafkaParser.py -F template.xml
`
It generates the bash file necessary to start the entire pipeline in the bash folder.
### 3. Start Zookeeper
To start the zookeeper element
`sh StartZookeeper.sh
`
### 4. Start Kafka Broker
To start a kafka broker
`sh StartKafka0.sh
`
### 5. Create Topics
Generate the topics with some specific configuration like log compaction
`sh CreateTopics.sh
`

### 6. Start Producer
To lunch a producer that allows to feed the pipeline
`sh StartProducer.sh
`
### 7. Start Streamers
At the end, start the streamer, that represent, each of them, a stage of the pipeline in a specific partition. For example:
`sh __Streamer__1996.0.0.sh
`
Start the streamer at stage 0 in the partition 0.


