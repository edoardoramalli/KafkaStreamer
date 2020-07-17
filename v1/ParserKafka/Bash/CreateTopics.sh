#!/usr/bin/env bash
sh /Users/edoardo/Documents/kafka/bin//kafka-topics.sh  --create  --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 2 --topic __stage_1996_0
sh /Users/edoardo/Documents/kafka/bin//kafka-topics.sh  --create  --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic __state_1996_0_0 --config cleanup.policy=compact  --config delete.retention.ms=10  --config segment.ms=10  --config min.cleanable.dirty.ratio=0 
sh /Users/edoardo/Documents/kafka/bin//kafka-topics.sh  --create  --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic __state_1996_0_1 --config cleanup.policy=compact  --config delete.retention.ms=10  --config segment.ms=10  --config min.cleanable.dirty.ratio=0 
sh /Users/edoardo/Documents/kafka/bin//kafka-topics.sh  --create  --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 2 --topic __stage_1996_1
sh /Users/edoardo/Documents/kafka/bin//kafka-topics.sh  --create  --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic __state_1996_1_0 --config cleanup.policy=compact  --config delete.retention.ms=10  --config segment.ms=10  --config min.cleanable.dirty.ratio=0 
sh /Users/edoardo/Documents/kafka/bin//kafka-topics.sh  --create  --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic __state_1996_1_1 --config cleanup.policy=compact  --config delete.retention.ms=10  --config segment.ms=10  --config min.cleanable.dirty.ratio=0 
sh /Users/edoardo/Documents/kafka/bin//kafka-topics.sh  --create  --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 2 --topic __stage_1996_2
sh /Users/edoardo/Documents/kafka/bin//kafka-topics.sh  --create  --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic __state_1996_2_0 --config cleanup.policy=compact  --config delete.retention.ms=10  --config segment.ms=10  --config min.cleanable.dirty.ratio=0 
sh /Users/edoardo/Documents/kafka/bin//kafka-topics.sh  --create  --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic __state_1996_2_1 --config cleanup.policy=compact  --config delete.retention.ms=10  --config segment.ms=10  --config min.cleanable.dirty.ratio=0 