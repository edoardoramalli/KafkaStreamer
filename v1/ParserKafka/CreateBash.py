
def create_bash_streamer(list_stage, partition, stream_id, jar_folder, list_streamer, bootstrap):
    for stage in list_stage:
        for part in range(partition):
            file_bash = "__Streamer__" + \
                        str(stream_id) + "." + \
                        str(stage) + "." + \
                        str(part) + ".sh"

            with open("./Bash/" + file_bash, "w+") as file:
                file.write("#!/usr/bin/env bash" + "\n")
                file.write("java -jar ")
                file.write(jar_folder)
                file.write("/Streamer.jar ")
                file.write("-f " + str(list_streamer[stage]["operation"]) + " ")
                file.write("-s " + str(stream_id) + " ")
                file.write("-i " + str(stage) + " ")
                if stage != list_stage[-1]:
                    file.write("-o " + str(stage + 1) + " ")
                file.write("-n " + str(part) + " ")
                file.write("-b " + str(bootstrap) + " ")
                file.write("\n")


def string_normal_topic(kafka_folder, zookeeper, replica, partition, topic_name):
    command = "sh "
    command += str(kafka_folder) + "/kafka-topics.sh "
    command += " --create "
    command += " --zookeeper " + str(zookeeper)
    command += " --replication-factor " + str(replica)
    command += " --partitions " + str(partition)
    command += " --topic " + str(topic_name)

    return command


def string_compact_topic(kafka_folder, zookeeper, replica, partition, topic_name):
    command = string_normal_topic(kafka_folder, zookeeper, replica, partition, topic_name)
    command += " --config cleanup.policy=compact "
    command += " --config delete.retention.ms=100 "
    command += " --config segment.ms=100 "
    command += " --config min.cleanable.dirty.ratio=0.01 "

    return command


def create_bash_topic(kafka_folder, list_stage, stream_id, zookeeper, replica, partition):
    list_stage = sorted(list_stage)
    with open("./Bash/" + "CreateTopics.sh", "w+") as file:
        file.write("#!/usr/bin/env bash" + "\n")
        for stage in list_stage:
            file.write(string_normal_topic(kafka_folder,
                                           zookeeper,
                                           replica,
                                           partition,
                                           "__stage_" + str(stream_id) + "_" + str(stage)))
            file.write("\n")
            for index in range(partition):
                file.write(string_compact_topic(kafka_folder,
                                                zookeeper,
                                                replica,
                                                1,
                                                "__state_" + str(stream_id) + "_" + str(stage) + "_" + str(index)))
                file.write("\n")


def create_bash_zookeeper(kafka_folder, properties_folder, bash_folder):
    command = "sh "
    command += str(kafka_folder) + "/zookeeper-server-start.sh "
    command += properties_folder + "/zookeeper.properties"
    with open(bash_folder + "/StartZookeeper.sh", "w") as file:
        file.write("#!/usr/bin/env bash" + "\n")
        file.write(command + "\n")


def create_bash_kafka_server(kafka_folder, kafka_config, index, bash_folder):
    command = "sh "
    command += str(kafka_folder) + "/kafka-server-start.sh "
    command += kafka_config
    with open(bash_folder + "/StartKafka" + str(index) + ".sh", "w") as file:
        file.write("#!/usr/bin/env bash" + "\n")
        file.write(command + "\n")


def create_producer(jar_folder, stream_id, initial_stage, bootstrap, bash_folder, wait=5000):
    command = "java -jar  "
    command += str(jar_folder) + "/Producer.jar "
    command += " -b " + str(bootstrap)
    command += " -o " + str(initial_stage)
    command += " -s " + str(stream_id)
    command += " -w " + str(wait)

    with open(bash_folder + "/StartProducer.sh", "w") as file:
        file.write("#!/usr/bin/env bash" + "\n")
        file.write(command + "\n")
