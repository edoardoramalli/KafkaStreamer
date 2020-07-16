def create_config_kafka_server(id, stream_id, partition, port, zookeeper, log, properties_folder, replica):
    config_file_name = properties_folder + "/kafka_" + str(id) + ".properties"

    with open("./Configuration/" + "kafka.properties", 'r') as file:
        filedata = file.read()

    properties = {'broker.id': str(id),
                  'log.dirs': log + '/' + str(stream_id) + "/" + str(id),
                  'num.partitions': str(partition),
                  'listeners': 'PLAINTEXT://:' + str(port),
                  'zookeeper.connect': zookeeper,
                  'offsets.topic.replication.factor': str(replica),
                  'transaction.state.log.replication.factor': str(replica)}

    for property in properties:
        name_property = property
        value_property = properties[property]
        filedata = filedata.replace(name_property + '=?', name_property + '=' + value_property)

    with open(config_file_name, 'w') as file:
        file.write(filedata)

    return config_file_name


def create_config_zookeeper(port, log):
    zookeeper_file_name = "zookeeper.properties"
    # Read in the file
    with open("./Configuration/" + zookeeper_file_name, 'r') as file:
        filedata = file.read()

    filedata = filedata.replace("clientPort=?", 'clientPort=' + str(port))

    properties = {'clientPort': str(port),
                  'dataDir': log + "/zookeeper"}

    for property in properties:
        name_property = property
        value_property = properties[property]
        filedata = filedata.replace(name_property + '=?', name_property + '=' + value_property)

    # Write the file out again
    with open('./Properties/' + zookeeper_file_name, 'w') as file:
        file.write(filedata)

    return zookeeper_file_name
