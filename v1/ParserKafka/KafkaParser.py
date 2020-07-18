import xml.etree.ElementTree as Et
import random
import time
import sys
import ipaddress
from CreateBash import *
from CreateProperties import *
import os
import argparse


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

        current_stage = int(streamer_properties['stage'])

        list_stage.append(current_stage)
        list_streamer[current_stage] = streamer_properties

    compare_list = list(range(0, max(list_stage) + 1))

    list_stage = sorted(list_stage)

    if list_stage != compare_list:
        missing = list(set(compare_list) - set(list_stage))
        raise ValueError("Missing stage:" + str(missing))

    return list_stage, list_streamer


def check_tag_string(tag, name, quantity):
    if len(tag) == quantity:
        if quantity == 1:
            return tag[0]
        else:
            return tag
    else:
        raise ValueError("Invalid" + name + " information")


def check_tag_int(tag, name, quantity):
    if len(tag) == quantity:
        return int(tag[0])
    else:
        raise ValueError("Invalid" + name + " information")


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

    # Folder System
    current_folder = os.path.abspath(os.getcwd())

    properties_folder = current_folder + "/Properties"
    if not os.path.exists(properties_folder):
        os.makedirs(properties_folder)

    bash_folder = current_folder + "/Bash"
    if not os.path.exists(bash_folder):
        os.makedirs(bash_folder)

    config_folder = current_folder + "/Configuration"
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

    # XML Tag General Info
    bootstrap = [bootstrap.attrib['value'] for bootstrap in stream.iter('bootstrap')]
    log = [log.attrib['value'] for log in stream.iter('log')]
    zookeeper = [zookeeper.attrib['value'] for zookeeper in stream.iter('zookeeper')]
    jar_folder = [jar_folder.attrib['value'] for jar_folder in stream.iter('jar')]
    kafka_folder = [kafka_folder.attrib['value'] for kafka_folder in stream.iter('kafka')]

    replica = [replica.attrib['value'] for replica in stream.iter('replica')]
    partition = [partition.attrib['value'] for partition in stream.iter('partition')]

    # Check Tags
    zookeeper = check_tag_ip(zookeeper, "zookeeper", 1)[0]
    partition = check_tag_int(partition, "partition", 1)
    jar_folder = check_tag_string(jar_folder, "jar_folder", 1)
    kafka_folder = check_tag_string(kafka_folder, "kafka_folder", 1)
    log = check_tag_string(log, "log", 1)
    replica = check_tag_int(replica, "replica", 1)
    bootstrap = check_tag_ip(bootstrap, "bootstrap", replica)

    # Parse Streamer
    list_stage, list_streamer = parse_stage(stream)

    # Create Zookeeper
    port_zookeeper = zookeeper.split(':')[1]
    create_config_zookeeper(port_zookeeper, log)
    create_bash_zookeeper(kafka_folder, properties_folder, bash_folder)

    # Create Kafka Server
    for index, server in enumerate(bootstrap):
        kafka_config = create_config_kafka_server(index,
                                                  stream_id,
                                                  partition,
                                                  server.split(":")[1],
                                                  zookeeper,
                                                  log,
                                                  properties_folder,
                                                  replica)
        create_bash_kafka_server(kafka_folder, kafka_config, index, bash_folder)

    create_bash_streamer(list_stage, partition, stream_id, jar_folder, list_streamer, bootstrap[0])
    create_bash_topic(kafka_folder, list_stage, stream_id, zookeeper, replica, partition)

    # Create Producer
    create_producer(jar_folder, stream_id, list_stage[0], bootstrap[0], bash_folder, partition)


def main(args):
    file = args["file"]
    parse_xml(file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(usage="DrugBank Parser")
    parser.add_argument("-F", "--file", required=True, help="XML file to be parsed")

    main(vars(parser.parse_args()))
