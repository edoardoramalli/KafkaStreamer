def create_config_kafka_server(stream_id, kafka_server_name, partition, socket, zookeeper, log, properties_folder, replica):
    config_file_name = properties_folder + "/" + str(kafka_server_name) + ".properties"

    with open("./Configuration/" + "kafka.properties", 'r') as file:
        filedata = file.read()

    properties = {'log.dirs': log + '/' + str(stream_id) + "/" + str(kafka_server_name),
                  'num.partitions': str(partition),
                  'listeners': 'PLAINTEXT://' + str(socket),
                  'advertised.listeners': 'PLAINTEXT://' + str(socket),
                  'zookeeper.connect': zookeeper,
                  'offsets.topic.replication.factor': str(replica),
                  'transaction.state.log.replication.factor': str(replica)}

    for property in properties:
        name_property = property
        value_property = properties[property]
        filedata = filedata.replace(name_property + '=?', name_property + '=' + value_property)

    with open(config_file_name, 'w+') as file:
        file.write(filedata)

    return config_file_name


def create_config_zookeeper(port, log, stream_id, zookeeper_properties):
    zookeeper_file_name = "zookeeper.properties"
    # Read in the file
    with open("./Configuration/" + zookeeper_file_name, 'r') as file:
        filedata = file.read()

    filedata = filedata.replace("clientPort=?", 'clientPort=' + str(port))

    properties = {'clientPort': str(port),
                  'dataDir': log + "/" + stream_id + "/zookeeper"}

    for property in properties:
        name_property = property
        value_property = properties[property]
        filedata = filedata.replace(name_property + '=?', name_property + '=' + value_property)

    # Write the file out again
    with open(zookeeper_properties + zookeeper_file_name, 'w') as file:
        file.write(filedata)

    return zookeeper_file_name
