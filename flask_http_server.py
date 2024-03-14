from flask import Flask, request, jsonify
import time
from raft import RaftNode

import logging 

# Configure the logging module
logging.basicConfig(filename='app.log', level=logging.DEBUG,
                    format='%(asctime)s [%(levelname)s] %(message)s')


app = Flask(__name__)
 
# Create the intercommunication json 
ip_addr = "192.168.136.128"
comm_dict = {"node0": {"ip": ip_addr, "port": "5567"}, 
            "node1": {"ip": ip_addr, "port": "5566"}, 
            "node2": {"ip": ip_addr, "port": "5565"},
            "node3": {"ip": ip_addr, "port": "5564"},}

# Start a few nodes
nodes = []

latest_offset = 0


# In-memory data structure to store records
metadata_store = {
    "RegisterBrokerRecords": {"records": [], "timestamp": []},
    "TopicRecords": {"records": [], "timestamp": []},
    "PartitionRecords": {"records": [], "timestamp": []},
    "ProducerIdsRecords": {"records": [], "timestamp": []},
}

def get_timestamp():
    import time
    # cur = datetime.now()
    # timestamp_in_minutes = (current_timestamp - datetime(1970, 1, 1)).total_seconds() / 60
    return int(time.time())

# Helper function to find a record by ID in a specific record type
def find_record_by_id(record_type, record_id):
    app.logger.info("Finding record by ID: %s", record_id)

    if record_type == "PartitionRecords":
        for record in metadata_store[record_type]["records"]:
            if record.get("partitionUUID") == record_id:
                new = record.get("epoch") + 1
                metadata_store[record_type]["records"][0]["epoch"] = new
                # print("Epoch Recorded: ", metadata_store[record_type]["records"][0]["epoch"]) # DEBUG
                return record
            
    for record in metadata_store[record_type]["records"]:
        if record.get("internalUUID") == record_id and record.get("brokerStatus") != "CLOSED":
            new = record.get("epoch") + 1
            metadata_store[record_type]["records"][0]["epoch"] = new
            # print("Epoch Recorded: ", metadata_store[record_type]["records"][0]["epoch"]) # DEBUG
            return record
        if record.get("internalUUID") == record_id and record.get("brokerStatus") == "CLOSED":
            return "NOPE" # meaning u r modifying closed broker which is not possible 
        
    return None

# to include new raftnodes into the cluster
@app.route('/api/new_node', methods=['POST'])
def new_node():

    app.logger.info("Adding new node to the cluster")

    data = request.json

    global comm_dict
    global nodes

    # get only the key
    name = list(data.keys())[0]

    comm_dict[name] = data[name]

    # print("comm_dict now", comm_dict) # DEBUG

    nodes.append(RaftNode(comm_dict, name))
    time.sleep(2)
    nodes[-1].start()
    # print("node data: ", data) # DEBUG
    print("all_nodes: ", nodes) # DEBUG

    nodes[-1].client_request({'val': metadata_store})
    time.sleep(5)
    
    for n in nodes:
        print("recent entry: ", n.check_committed_entry())

    return jsonify({"status": "successfully added to the cluster"})



# to remove raftnodes from the cluster
@app.route('/api/remove_node', methods=['POST'])
def remove_node():

    global nodes

    app.logger.info("Removing node from the cluster")

    global comm_dict
    global nodes

    data = request.json
    # get only the key
    name = list(data.keys())[0]

    for n in nodes:
        if n.name == name:
            n.stop()
            nodes.remove(n)
            del comm_dict[name]
            # print("comm_dict now", comm_dict) # DEBUG
            # print("removed node: ", name) # DEBUG
            # print("present nodes are: ", nodes) # DEBUG
    
    nodes[0].client_request({'val': metadata_store})
    time.sleep(5)
    
    for n in nodes:
        print("recent entry: ", n.check_committed_entry())

    return jsonify({"status": "successfully removed"})



# RegisterBrokerRecord API
@app.route('/api/register-broker-record', methods=['POST'])
def register_broker_record():
    app.logger.info("Registering broker record")
    data = request.json
    record_type = "RegisterBrokerRecords"
    record_id = data.get("internalUUID")

    global nodes

    # Check if the broker record with the given ID already exists
    existing_record = find_record_by_id(record_type, record_id)
    if existing_record != "NOPE" and existing_record != None:
        # Update the existing record
        existing_record.update(data)
        # Update the timestamp
        metadata_store[record_type]["timestamp"].append(get_timestamp())
    
    elif existing_record == None:
        # Add a new record
        metadata_store[record_type]["records"].append(data)
        # Update the timestamp
        metadata_store[record_type]["timestamp"].append(get_timestamp())
    else:
        pass

    latest_offset = metadata_store[record_type]["timestamp"][-1]

    nodes[0].client_request({'val': metadata_store})
    time.sleep(5)
    
    for n in nodes:
        print("recent entry: ", n.check_committed_entry())

    return jsonify( metadata_store["RegisterBrokerRecords"])


# TopicRecord API
@app.route('/api/topic-record', methods=['POST'])
def create_topic_record():
    app.logger.info("Creating topic record")
    data = request.json
    record_type = "TopicRecords"
    record_id = data.get("topicUUID")

    # Check if the topic record with the given ID already exists
    existing_record = find_record_by_id(record_type, record_id)
    if existing_record:
        return jsonify({"status": "error", "message": "Topic with the same UUID already exists"})

    # Add a new record
    metadata_store[record_type]["records"].append(data)

    # Update the timestamp
    metadata_store[record_type]["timestamp"].append(get_timestamp())
    print("Topic Recorded: ", metadata_store[record_type]["records"]) # DEBUG

    latest_offset = metadata_store[record_type]["timestamp"][-1]

    nodes[0].client_request({'val': metadata_store})
    time.sleep(5)
    
    for n in nodes:
        print("recent entry: ", n.check_committed_entry())

    return jsonify(metadata_store['TopicRecords'])


def change_status(leader):
    app.logger.info("Changing status of the broker")
    for record in metadata_store["RegisterBrokerRecords"]["records"]:
        if record.get("brokerId") == leader:
            record["brokerStatus"] = "ALIVE"
        else:
            record["brokerStatus"] = "INIT"


# Additional API endpoints for other record types...
@app.route('/api/partition-record', methods=['POST'])
def create_partition_record():
    app.logger.info("Creating partition record")
    data = request.json
    record_type = "PartitionRecords"
    record_id = data.get("partitionUUID")

    leader = data.get("leader")
    change_status(leader)


    # Check if the partition record with the given ID already exists
    existing_record = find_record_by_id(record_type, record_id)
    if existing_record:
        existing_record.update(data)
    else:
        # Add a new record
        metadata_store[record_type]["records"].append(data)

    # Update the timestamp
    metadata_store[record_type]["timestamp"].append(get_timestamp())

    # print("Partition Recorded: ", metadata_store[record_type]["records"]) # DEBUG

    latest_offset = metadata_store[record_type]["timestamp"][-1]

    nodes[0].client_request({'val': metadata_store})
    time.sleep(5)
    
    for n in nodes:
        print("recent entry: ", n.check_committed_entry())

    return jsonify(metadata_store['PartitionRecords'])


# Register producer ID record and requesting broker ID
@app.route('/api/producer-id-record', methods=['POST'])
def create_producer_id_record():
    app.logger.info("Creating producer ID record")
    data = request.json
    record_type = "ProducerIdsRecords"
    record_id = data.get("producerIdUUID")

    # Check if the producer ID record with the given ID already exists
    existing_record = find_record_by_id(record_type, record_id)
    if existing_record:
        return jsonify({"status": "error", "message": "Producer ID with the same UUID already exists"})

    # Add a new record
    metadata_store[record_type]["records"].append(data)

    # Update the timestamp
    metadata_store[record_type]["timestamp"].append(get_timestamp())

    print("metadata_store: ", metadata_store )# DEBUG

    latest_offset = metadata_store[record_type]["timestamp"][-1]

    nodes[0].client_request({'val': metadata_store})
    time.sleep(5)
    
    for n in nodes:
        print("recent entry: ", n.check_committed_entry())

    return jsonify(metadata_store['ProducerIdsRecords'])



# broker-mgmt endpoint
@app.route('/api/broker-mgmt', methods=['POST', 'GET'])
def broker_mgmt():
    app.logger.info("Broker management")
    data = request.json
    offset = data.get("timestamp")

    diff = {"diff_broker": [],
            "diff_topic": [],
            "diff_partition": []}
    print(metadata_store["RegisterBrokerRecords"]["timestamp"])

    for data in metadata_store["RegisterBrokerRecords"]["timestamp"]:
        if data-offset > 300:
            return jsonify(metadata_store)
        elif data-offset>0 and data-offset<=300:
            diff["diff_broker"].append(data-offset)
    
    for data in metadata_store["TopicRecords"]["timestamp"]:
        if data-offset > 300:
            return jsonify(metadata_store)
        elif data-offset>0 and data-offset<=300:
            diff["diff_topic"].append(data-offset)
    
    for data in metadata_store["PartitionRecords"]["timestamp"]:
        if data-offset > 300:
            return jsonify(metadata_store)
        elif data-offset>0 and data-offset<=300:
            diff["diff_partition"].append(data-offset)
    
    return jsonify(diff)


# client-mgmt endpoint
@app.route('/api/client-mgmt', methods=['POST'])
def client_mgmt():
    app.logger.info("Client management")
    data = request.json
    offset = data.get("timestamp")

    diff = {"brokers": [],
            "topics": [],
            "partitions": []}
    
    print("IDKKK", metadata_store["RegisterBrokerRecords"])

    for i in range(len(metadata_store["RegisterBrokerRecords"]['timestamp'])):
        data = metadata_store["RegisterBrokerRecords"]['timestamp'][i]
        if data-offset > 300:
            return jsonify(metadata_store)
        elif data-offset>0 and data-offset<=300:
            x = metadata_store["RegisterBrokerRecords"]["records"][i].get("brokerId")
            diff["brokers"].append(x)
    
    for i in range(len(metadata_store["TopicRecords"]['timestamp'])):
        data = metadata_store["TopicRecords"]['timestamp'][i]
        if data-offset > 300:
            return jsonify(metadata_store)
        elif data-offset>0 and data-offset<=300:
            x = metadata_store["TopicRecords"]["records"][i].get("topicUUID")
            diff["topics"].append(x)
    
    for i in range(len(metadata_store["PartitionRecords"]['timestamp'])):
        data = metadata_store["PartitionRecords"]['timestamp'][i]
        if data-offset > 300:
            return jsonify(metadata_store)
        elif data-offset>0 and data-offset<=300:
            x = metadata_store["PartitionRecords"]["records"][i].get("partitionId")
            diff["partitions"].append(x)
    
    return jsonify(diff)




# Get all records of a specific type
@app.route('/api/get-records-from-nodes', methods=['POST'])
def node_records():
    app.logger.info("Getting all records of a specific node")
    data = request.json

    global comm_dict
    global nodes

    # get only the key
    name = data.get("name")

    print("name", name)


    for i in range(len(nodes)):
        if nodes[i]._name == name:
            print("it works")
            ans = nodes[i].check_committed_entry()

    return jsonify(ans)


def default_route():
    app.logger.info("Starting ...")
    global nodes
    for name, address in comm_dict.items():
        nodes.append(RaftNode(comm_dict, name))
        nodes[-1].start()  # last node meaning append nodes to the list start one by one

    # Let a leader emerge
    # time.sleep(5)

    # nodes[0].stop()
    # print("stopped node")

    # # remove node form comm_dict
    # del comm_dict["node0"]
    # print("comm_dict now", comm_dict) # DEBUG

    # Make some requests
    # for val in range(5):
    #     nodes[0].client_request({'val': val})
    # time.sleep(5)

    # # Check and see what the most recent entry is
    # for n in nodes:
    #     print(n.check_committed_entry())

    # time.sleep(3)

    # nodes[0]._send_message({'val': 1})

    # testing log replication
    for val in range(2):
        nodes[0].client_request({'val': val})
    time.sleep(5)

    # for val in range(2):
    #     nodes[0].client_request({'val': val})
    # time.sleep(5)
    
    # latest commit entry of each node
    for n in nodes:
        print("recent entry: ", n.check_committed_entry())


    time.sleep(2)

    
    # data = nodes[0]._load_config(comm_dict)
    # print("loaded data: ", data)

@app.route('/')
def before_first_request() :
        default_route()
        return 'before_first_request'


if __name__ == '__main__':

    # start the flask server
    app.run(debug=True)
