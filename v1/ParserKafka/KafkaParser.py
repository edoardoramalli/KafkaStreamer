import xml.etree.ElementTree as Et
import random
import time
import sys
import ipaddress
from CreateBash import *
from CreateProperties import *
import os
import argparse


def parse_server(root):
    list_server_socket = {}
    list_server_log = {}
    list_server_properties = {}
    list_server_kafka = {}

    list_server_name = []

    for server in root.iter('Server'):
        server_properties = {prop.tag: prop.text for prop in server}

        # Check log
        if "log" not in server_properties:
            raise ValueError("No log tag found in 'Server' element")

        # Check name
        if "name" not in server_properties:
            raise ValueError("No name tag found in 'Server' element")

        # Check log
        if "kafka" not in server_properties:
            raise ValueError("No kafka tag found in 'Server' element")

        # Check log
        if "socket" not in server_properties:
            raise ValueError("No log tag found in 'Server' element")

        # Check log
        if "properties" not in server_properties:
            raise ValueError("No properties tag found in 'Server' element")

        current_name = server_properties["name"]

        if current_name in list_server_name:
            raise ValueError("Name Kafka Server already exists")
        else:
            list_server_name.append(current_name)

        socket = server_properties["socket"]
        check_socket(socket)
        list_server_socket[current_name] = socket
        list_server_log[current_name] = server_properties["log"]
        list_server_properties[current_name] = server_properties["properties"]
        list_server_kafka[current_name] = server_properties["kafka"]

    return list_server_name, list_server_properties, list_server_socket, list_server_kafka, list_server_log


def parse_zookeeper(root):
    for zookeeper in root.iter('Zookeeper'):
        zookeeper_properties = {prop.tag: prop.text for prop in zookeeper}

        # Check log
        if "log" not in zookeeper_properties:
            raise ValueError("No log tag found in 'Zookeeper' element")

        # Check log
        if "kafka" not in zookeeper_properties:
            raise ValueError("No kafka tag found in 'Zookeeper' element")

        # Check log
        if "socket" not in zookeeper_properties:
            raise ValueError("No log tag found in 'Zookeeper' element")

        # Check log
        if "properties" not in zookeeper_properties:
            raise ValueError("No properties tag found in 'Zookeeper' element")

        zookeeper_socket = zookeeper_properties["socket"]
        check_socket(zookeeper_socket)
        zookeeper_kafka = zookeeper_properties["kafka"]
        zookeeper_log = zookeeper_properties["log"]
        zookeeper_properties = zookeeper_properties["properties"]

        return zookeeper_socket, zookeeper_kafka, zookeeper_log, zookeeper_properties


def parse_producer(root):
    for producer in root.iter('Producer'):
        producer_properties = {prop.tag: prop.text for prop in producer}
        # Check jar file
        if "jar" not in producer_properties:
            raise ValueError("No jar tag found in 'Producer' element")
        # Check wait time
        if "wait" not in producer_properties:
            raise ValueError("No wait tag found in 'Producer' element")

        wait_time = int(producer_properties['wait'])
        jar_file = producer_properties['jar']

        return jar_file, wait_time


def parse_topic(root):
    for topic in root.iter('Topic'):
        topic_properties = {prop.tag: prop.text for prop in topic}
        # Check jar file
        if "kafka" not in topic_properties:
            raise ValueError("No kafka tag found in 'Topic' element")
        # Check wait time
        if "socket" not in topic_properties:
            raise ValueError("No socket tag found in 'Topic' element")

        socket = topic_properties["socket"]
        check_socket(socket)

        return socket, topic_properties["kafka"]


def parse_stage(root):
    list_streamer = {}
    list_stage = []
    list_possible_function = ["adder", "power", "diff", "identity"]

    for streamer in root.iter('Streamer'):
        streamer_properties = {prop.tag: prop.text for prop in streamer}

        # Check ID Stage
        if "stage" not in streamer_properties:
            raise ValueError("No stage tag found in 'Streamer' element")

        # Check function Streamer
        if "operation" not in streamer_properties:
            raise ValueError("No function tag found in 'Streamer' element")
        else:
            if streamer_properties['operation'] not in list_possible_function:
                raise ValueError("Function not allowed in operation tag")

        # Check function Streamer
        if "jar" not in streamer_properties:
            raise ValueError("No jar file found in 'Streamer' element")

        current_stage = int(streamer_properties['stage'])

        list_stage.append(current_stage)
        list_streamer[current_stage] = streamer_properties

    compare_list = list(range(0, max(list_stage) + 1))

    list_stage = sorted(list_stage)

    if list_stage != compare_list:
        missing = list(set(compare_list) - set(list_stage))
        raise ValueError("Missing stage: " + str(missing))

    return list_stage, list_streamer


def check_tag_string(tag, name, quantity):
    if len(tag) == quantity:
        if quantity == 1:
            return tag[0]
        else:
            return tag
    else:
        raise ValueError("Invalid " + name + " information")


def check_tag_int(tag, name, quantity):
    if len(tag) == quantity:
        return int(tag[0])
    else:
        raise ValueError("Invalid " + name + " information")


def check_socket(socket):
    ipaddress.ip_address(socket[:socket.index(':')])


def check_tag_ip(tag, name, quantity):
    for server in tag:
        if not server.count(':') == 1:
            raise ValueError("Invalid" + name + " information")
        ipaddress.ip_address(server[:server.index(':')])

    if len(tag) == quantity:
        return tag
    else:
        raise ValueError("Invalid" + name + " information")


def parse_xml(filename):
    stream = Et.parse(filename).getroot()

    # Output and Template Folder
    output = [output.attrib['value'] for output in stream.iter('output')]
    configuration = [configuration.attrib['value'] for configuration in stream.iter('configuration')]

    output = check_tag_string(output, "output", 1)
    configuration = check_tag_string(configuration, "configuration", 1)

    bash_folder = output + "/Bash/"
    if not os.path.exists(bash_folder):
        os.makedirs(bash_folder)

    config_folder = configuration
    if not os.path.exists(config_folder):
        raise ValueError("No configuration template folder found")

    # Check Root
    if stream.tag != 'Stream':
        raise ValueError("No 'Stream' tag root element found")

    # Get Root ID or take a random one
    if 'id' in stream.attrib:
        stream_id = stream.attrib['id']
    else:
        random.seed(int(time.time()))
        stream_id = random.randint(0, sys.maxsize)

    replica = [replica.attrib['value'] for replica in stream.iter('replica')]
    partition = [partition.attrib['value'] for partition in stream.iter('partition')]

    # Check Tags
    partition = check_tag_int(partition, "partition", 1)
    replica = check_tag_int(replica, "replica", 1)

    # Parse Zookeeper
    zookeeper_socket, zookeeper_kafka, zookeeper_log, zookeeper_properties = parse_zookeeper(stream)

    # Parser Server
    list_server_name, list_server_properties, list_server_socket, list_server_kafka, list_server_log = parse_server(
        stream)

    # Parse Streamer
    list_stage, list_streamer = parse_stage(stream)

    # Parse Producer
    jar_file_producer, wait_time_producer = parse_producer(stream)

    # Parse Topic
    topic_socket, topic_kafka = parse_topic(stream)

    # Create Kafka Server
    if len(list_server_name) != replica:
        raise ValueError("Replica number is different from number of kafka server")

    for index, server in enumerate(list_server_name):
        create_config_kafka_server(stream_id,
                                   list_server_name[index],
                                   partition,
                                   list_server_socket[server],
                                   zookeeper_socket,
                                   list_server_log[server],
                                   list_server_properties[server],
                                   replica)
        create_bash_kafka_server(list_server_kafka[server],
                                 list_server_properties[server],
                                 list_server_name[index],
                                 bash_folder)

    create_bash_streamer(list_stage, partition, stream_id, list_streamer, list_server_socket[list_server_name[0]],
                         bash_folder)
    create_bash_topic(topic_kafka, list_stage, stream_id, topic_socket, replica, partition, bash_folder)

    # Create Producer
    create_producer(jar_file_producer, stream_id, list_stage[0], list_server_socket[list_server_name[0]],
                    bash_folder, partition, wait_time_producer)

    create_config_zookeeper(zookeeper_socket.split(":")[1], zookeeper_log, stream_id, zookeeper_properties)
    create_bash_zookeeper(zookeeper_kafka, zookeeper_properties, bash_folder)


def main(args):
    file = args["file"]
    parse_xml(file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(usage="DrugBank Parser")
    parser.add_argument("-F", "--file", required=True, help="XML file to be parsed")

    main(vars(parser.parse_args()))
