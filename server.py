import time
import shutil
import base64
import threading
import json
import pika
import os
import flask
from hashlib import sha256
from flask import Flask, Response, request, jsonify
import socket
from threading import Thread, Lock

import argparse


app = Flask(__name__)

# Setup argument parser
parser = argparse.ArgumentParser(
    description='Start a server for a distributed file sharing system.')
parser.add_argument('-p', '--port', type=int,
                    help='Port number to run the Flask app on', required=True)
args = parser.parse_args()

# Load the configuration file


def load_config():
    with open('config.json', 'r') as config_file:
        return json.load(config_file)


def get_ip_address():
    return socket.gethostbyname(socket.gethostname())

# Find the configuration of this server using IP address


def find_self_config(all_configs, ip_address):
    print(all_configs, ip_address)
    for config in all_configs['participant_servers']:

        if config['ip'] == ip_address and config['port'] == args.port:
            print("correct", config)
            return config
    return None


file_list_responses = {}
file_list_responses_lock = Lock()
file_list_request_timeout = 10  # seconds
election_timeout = 5  # seconds
in_election_process = False
election_response_received = False

# Global Variables
known_nodes = set()
discovery_queue = 'node_discovery_queue'

# Send Discovery Message


def broadcast_discovery():
    message = json.dumps({'action': 'discover', 'node_id': self_node_id})
    send_message_to_queue(message, discovery_queue)

# Handle Discovery Response


def handle_discovery_response(response):
    global known_nodes
    node_info = json.loads(response)
    known_nodes.add(node_info['node_id'])
    print(f"Discovered node: {node_info['node_id']}")

# Setup RabbitMQ for Discovery


def setup_discovery_queue(node_id):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbitmq_config['host'], port=rabbitmq_config['port']))
    channel = connection.channel()
    channel.queue_declare(queue=discovery_queue)

    def discovery_callback(ch, method, properties, body):
        message = json.loads(body.decode())
        if 'action' in message and message['action'] == 'discover' and message['node_id'] != node_id:
            response = json.dumps({'node_id': node_id})
            send_message_to_queue(response, discovery_queue)

    channel.basic_consume(queue=discovery_queue,
                          on_message_callback=discovery_callback, auto_ack=True)
    channel.start_consuming()


# Start Discovery Process



consensus_proposed_values = []


def request_file_list_from_nodes(node_id):
    # Clear previous responses
    global file_list_responses
    file_list_responses = {}

    # Request a list of files from all other nodes
    for server in participant_servers:
        if server['node_id'] != node_id:
            send_message_to_queue(
                "Request file list", f"file_list_request_queue_{server['node_id']}")


def handle_file_list_request(node_id):
    # Handle request to send list of files
    files = os.listdir(f'files_node_{node_id}')
    file_list_message = json.dumps({'node_id': node_id, 'files': files})
    send_message_to_queue(file_list_message, 'file_list_response_queue')


def aggregate_file_lists():
    with file_list_responses_lock:
        return {node_id: data['files'] for node_id, data in file_list_responses.items()}

# Calculate SHA-256 hash of a file


def calculate_file_hash_from_content(file_content):
    return sha256(file_content).hexdigest()


def handle_storage_info_message(body):
    global node_storage_info
    message_data = json.loads(body)
    node_id = message_data['node_id']
    storage_info = message_data['storage_info']

    # Update the global storage info dictionary
    node_storage_info[node_id] = storage_info
    print(f"Updated storage info for node {node_id}: {storage_info}")


def handle_storage_info_request():
    storage_info = calculate_storage_info()

    send_storage_info_to_coordinator(storage_info)


 # Global dictionary to hold storage information of each node
node_storage_info = {}


def send_storage_info_to_coordinator(storage_info):
    global rabbitmq_config, leader_node_id

    if leader_node_id is None:
        print("Leader node ID not set. Cannot send storage info.")
        return

    message = json.dumps({
        'node_id': self_node_id,
        'storage_info': storage_info
    })

    send_message_to_queue(
        message, f"storage_info_response_queue_{leader_node_id}")
    print(
        f"Sending storage info to coordinator from node {self_node_id}: {storage_info}")


def request_storage_info_from_nodes():
    global participant_servers
    for server in participant_servers:
        if server['node_id'] != self_node_id:
            send_message_to_queue(
                "Request storage info", f"storage_info_request_queue_{server['node_id']}")


def get_total_storage():
    """ Returns the total storage capacity of the node in gigabytes. """
    total, _, _ = shutil.disk_usage("/")
    return total // (2**30)  # Convert bytes to gigabytes


def get_used_storage():
    """ Returns the used storage of the node in gigabytes. """
    _, used, _ = shutil.disk_usage("/")
    return used // (2**30)  # Convert bytes to gigabytes


def calculate_storage_info():
    total_storage = get_total_storage()
    used_storage = get_used_storage()
    return {
        'total_storage': total_storage,
        'used_storage': used_storage
    }


def select_nodes_for_replication(file_size, replication_factor=2):
    suitable_nodes = []
    for node_id, storage_info in node_storage_info.items():
        print(node_storage_info)
        available_storage = storage_info['total_storage'] - \
            storage_info['used_storage']
        print("Available storage ", available_storage)
        if available_storage >= file_size:
            suitable_nodes.append((node_id, available_storage))

    print(f"Suitable nodes found: {suitable_nodes}")  # Debugging line

    if len(suitable_nodes) < replication_factor:
        print("Not enough suitable nodes found for replication.")  # Debugging line
        return None

    suitable_nodes.sort(key=lambda x: x[1], reverse=True)
    selected_nodes = [node[0] for node in suitable_nodes[:replication_factor]]
    # Debugging line
    print(f"Selected nodes for replication: {selected_nodes}")
    return selected_nodes


# Replicate file to other nodes
def replicate_file(file_name, content):
    # Encode the binary content to base64 string
    base64_content = base64.b64encode(content).decode('utf-8')
    file_hash = calculate_file_hash_from_content(content)

    replication_message = json.dumps({
        'action': 'replicate',
        'file_name': file_name,
        'content': base64_content,  # Use the base64 encoded content
        'hash': file_hash
    })

    # Send the replication message to other nodes
    for server in participant_servers:
        if server['node_id'] != self_node_id and (not is_coordinator() or server['node_id'] != leader_node_id):

            send_message_to_queue(replication_message,
                                  f"file_operations_queue_{server['node_id']}")

# Function to send message to a specific RabbitMQ queue


def send_message_to_queue(message, queue_name):
    print(f"Sending message to {queue_name}: {message}")
    global rabbitmq_config
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbitmq_config['host'], port=rabbitmq_config['port']))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    connection.close()


def is_coordinator():
    global self_node_id, leader_node_id
    return self_node_id == leader_node_id


# Flask route for uploading files
@app.route('/upload', methods=['POST'])
def upload_file():
    if not is_coordinator():
        return jsonify({"error": "Uploads must be sent to the coordinator node"}), 403

    file = request.files['file']
    filename = file.filename
    content = file.read()
    file_size = len(content) / (1024 * 1024)  # Size in MB

    # Request storage information from all nodes
    request_storage_info_from_nodes()

    # Wait for a brief period to collect storage info from nodes
    time.sleep(5)

    # Select nodes for replication
    target_node_ids = select_nodes_for_replication(file_size)
    if target_node_ids is None:
        return jsonify({"error": "No suitable nodes found for file replication"}), 500

    # Initiate file replication on selected nodes
    for node_id in target_node_ids:
        initiate_file_replication(filename, content, node_id)

    return jsonify({"message": "File upload and replication initiated on multiple nodes"}), 200


def initiate_file_replication(file_name, file_content, target_node_id):
    # Encode the file content to base64 to ensure safe transmission through RabbitMQ
    base64_content = base64.b64encode(file_content).decode('utf-8')

    # Prepare the replication message
    replication_message = json.dumps({
        'action': 'replicate',
        'file_name': file_name,
        'content': base64_content,  # Encoded file content
        'hash': calculate_file_hash_from_content(file_content)
    })

    # Send the replication message to the target node's queue
    send_message_to_queue(replication_message,
                          f"file_operations_queue_{target_node_id}")


@app.route('/download', methods=['GET'])
def download_file():
    filename = request.args.get('filename')
    if not filename:
        return jsonify({"error": "Filename is required"}), 400

    # Query nodes for the file
    file_location_info = query_nodes_for_file(filename)
    if file_location_info is None:
        return jsonify({"error": "File not available"}), 404

    # Retrieve file from the node that has it
    file_content = retrieve_file_from_node(file_location_info, filename)
    if file_content:
        return Response(file_content, mimetype='application/octet-stream')
    else:
        return jsonify({"error": "Failed to retrieve file"}), 500

# Define a timeout for file query responses (in seconds)
file_query_timeout = 15
 
def query_nodes_for_file(filename):
    query_message = json.dumps({'action': 'query_file', 'filename': filename})
    broadcast_message_to_all_nodes(query_message, 'file_query_queue')

    # Implement a waiting mechanism here to wait for responses
    time.sleep(file_query_timeout)

    # After waiting, process the responses to find the best node for download
    return process_file_query_responses()
def handle_file_query_message(ch, method, properties, body):
    global file_query_responses
    message = json.loads(body)
    filename = message['filename']
    file_path = os.path.join(file_dir, filename)
    
    response = {'file_available': False}
    if os.path.exists(file_path):
        response = {'file_available': True}
    
    # Store or send the response
    file_query_responses[self_node_id] = response
    # If using a response queue, send the response to that queue
    response_message = json.dumps(response)
    send_message_to_queue(response_message, 'file_query_response_queue')

def handle_file_retrieve_message(ch, method, properties, body):
    message = json.loads(body)
    filename = message['filename']
    file_path = os.path.join(file_dir, filename)
    if os.path.exists(file_path):
        with open(file_path, 'rb') as file:
            file_content = file.read()
            # Send the file content to the coordinator
            send_file_content_to_coordinator(file_content)
def send_file_content_to_coordinator(file_content, filename):
    global leader_node_id
    # Encode the file content to base64 to ensure safe transmission
    encoded_content = base64.b64encode(file_content).decode('utf-8')

    # Prepare the message
    file_data_message = json.dumps({
        'action': 'file_data',
        'filename': filename,
        'content': encoded_content,
        'node_id': self_node_id
    })
    send_message_to_queue(file_data_message, f'file_data_queue_{leader_node_id}')



global received_file_content, file_content_received, filename_waiting_for
filename_waiting_for = None


    
def handle_file_data_message(ch, method, properties, body):
    global received_file_content, file_content_received
    message = json.loads(body)
    
    if message['action'] == 'file_data' and message['filename'] == filename_waiting_for:
        # Decode the file content from base64
        received_file_content = base64.b64decode(message['content'])
        file_content_received.set()  # Signal that file content has been received
        
file_query_responses = {}
def process_file_query_responses():
    best_node = None
    for node_id, response in file_query_responses.items():
        if response['file_available']:
            best_node = node_id  # Simplified selection logic
            break

    if best_node is not None:
        return {
            'node_id': best_node,
            'filename': response['filename']
        }
    else:
        return None
    
def broadcast_message_to_all_nodes(message, queue_name):
    for node_id in known_nodes:
        send_message_to_queue(message, f'{queue_name}_{node_id}')
def retrieve_file_from_node(node_info, filename):
    global filename_waiting_for
    filename_waiting_for = filename

    request_message = json.dumps({'action': 'retrieve_file', 'filename': filename})
    send_message_to_queue(request_message, f'file_retrieve_queue_{node_info["node_id"]}')

    # Wait for the file content
    return wait_for_file_content(filename)

def receive_file_content_from_node(node_info, filename):
    """
    Receives the file content from a specified node.

    :param node_info: Information about the node from which the file is to be retrieved.
    :param filename: The name of the file to be retrieved.
    """
    # Assuming node_info contains the necessary details to connect to the node
    node_id = node_info['node_id']

    # Prepare the request message for file retrieval
    request_message = json.dumps({'action': 'retrieve_file', 'filename': filename})

    # Send the request to the specific node's queue for file retrieval
    send_message_to_queue(request_message, f'file_retrieve_queue_{node_id}')

    # Wait for the response with the file content
    # Assuming there's a mechanism to wait and receive the file content
    # This could be a blocking call until the file content is received or a timeout occurs
    file_content = wait_for_file_content(filename)

    if file_content:
        # File content received successfully
        return file_content
    else:
        # Handle the case where file content is not received (e.g., timeout or error)
        print(f"Failed to receive file content for '{filename}' from node {node_id}")
        return None

def wait_for_file_content(filename):
    global file_content_received, received_file_content, filename_waiting_for

    file_content_received.clear()  # Reset the event
    filename_waiting_for = filename  # Set the filename we are waiting for

    # Wait for the file content to be received or for a timeout
    file_content_received.wait(timeout=30)  # Timeout in seconds, adjust as needed

    if file_content_received.is_set():
        return received_file_content  # Return the received file content
    else:
        return None  # Return None if timeout occurs


# Flask route for listing files


@app.route('/list', methods=['GET'])
def list_files():
    if self_node_id == leader_node_id:
        request_file_list_from_nodes(self_node_id)
        time.sleep(10)  # Increase the wait time
        aggregated_file_list = aggregate_file_lists()
        return jsonify(aggregated_file_list), 200

    else:
        return jsonify({"error": "This node is not the leader"}), 403


# Handle replication request from other nodes
def handle_replication_request(data):
    if is_coordinator():
        # Coordinator node does not store files
        return
    file_path = os.path.join(file_dir, data['file_name'])

    # Decode the content from base64
    content = base64.b64decode(data['content'])

    with open(file_path, 'wb') as f:
        f.write(content)

    if calculate_file_hash_from_content(content) != data['hash']:
        print(f"Consistency error in replicating {data['file_name']}")


leader_node_id = None


def send_election_message(to_node_id):
    election_message = json.dumps(
        {'type': 'election', 'from_node_id': self_node_id})
    send_message_to_queue(election_message, f"election_queue_{to_node_id}")


leader_node_id = None


def process_election_message(from_node_id, current_node_id):
    if from_node_id < current_node_id:
        # Respond to the election message indicating a higher node is active
        send_election_message(from_node_id)
        # Start a new election
        start_bully_election(current_node_id)


def handle_election_message(from_node_id):
    global leader_node_id

        # Respond and start own election
    send_election_message(from_node_id)
    start_bully_election(self_node_id)

        # Ignore if the node ID is lower
        #print(f"Node {self_node_id} ignoring election message from lower ID node {from_node_id}.")



def announce_leader(node_id):
    global consensus_leader_node_id
    consensus_leader_node_id = node_id
    for server in participant_servers:
        if server['node_id'] != node_id:
            send_message_to_queue(
                f"Leader {node_id}", f"leader_announcement_queue_{server['node_id']}")
    print(f"Consensus reached: Node {node_id} is the leader!")


def request_consensus_values_from_nodes(node_id):
    # Request proposed consensus values from all other nodes
    for server in participant_servers:
        if server['node_id'] != node_id:
            send_message_to_queue("Request consensus value",
                                  f"consensus_queue_{server['node_id']}")


def handle_consensus_proposal(message):
    # Handle proposed consensus value from another node
    global consensus_proposed_values
    consensus_value = message.split()[-1]
    print(
        f"Received proposed consensus value: {consensus_value} from another node")
    consensus_proposed_values.append(consensus_value)


def start_bully_election(current_node_id):
    print(f"Starting Bully election from node {current_node_id}")
    global leader_node_id, in_election_process, election_response_received, consensus_proposed_values
    higher_nodes = [
        server for server in participant_servers if server['node_id'] > current_node_id]
    # Start the election process
    in_election_process = True
    election_response_received = False
    consensus_proposed_values = []  # Reset proposed values for a new election

    # Send election messages to higher-ranked nodes
    for node in higher_nodes:
        send_election_message(node['node_id'])

    # Wait for responses or timeout
    start_time = time.time()

    while not election_response_received and (time.time() - start_time) < election_timeout:
        time.sleep(0.5)

    if election_response_received:
        # Election was successful, and this node is not the leader
        in_election_process = False
        print(f"Node {self_node_id} lost the election.")
    else:
        # No response received, this node is the leader
        leader_node_id = current_node_id

        # Propose a consensus value (you can set it to a default value)
        consensus_value = "ProposedConsensusValue"
        consensus_proposed_values.append(consensus_value)

        announce_leader(current_node_id)
        print(
            f"Node {self_node_id} is the leader. Consensus Value: {consensus_value}")
        # Log that consensus happened
        print(
            f"Node {self_node_id}: Consensus happened. Applying changes... (for example, updating shared state)")

        # Check if there are proposed consensus values from other nodes
        if consensus_proposed_values:
            # Determine the final consensus value (simple approach: use the leader's proposal)
            final_consensus_value = consensus_proposed_values[0]
            print(f"Final Consensus Value: {final_consensus_value}")

        # Reset the election process variables
        in_election_process = False
        election_response_received = False


#General Consesus
consensus_responses = {}
consensus_lock = threading.Lock()
consensus_result = None
consensus_queue = 'consensus_queue'
def initiate_consensus(file_name):
    global consensus_responses, consensus_result
    consensus_responses = {}
    consensus_result = None
    file_version = get_file_version(file_name)
    proposal = {'file_name': file_name, 'node_id': self_node_id, 'version': file_version}
    message = json.dumps({'action': 'propose', 'proposal': proposal})
    broadcast_message_to_all_nodes(message, consensus_queue)
    
def broadcast_message_to_all_nodes(message, queue_name):
    for node_id, node_info in known_nodes.items():
        send_message_to_queue(message, f'{queue_name}_{node_id}')
        
def handle_consensus_message(ch, method, properties, body):
    message = json.loads(body)
    if message['action'] == 'propose':
        response = evaluate_consensus_proposal(message['proposal'])
        response_message = json.dumps({'action': 'response', 'response': response, 'responder_id': self_node_id})
        send_message_to_queue(response_message, f'{consensus_queue}_{message["proposal"]["node_id"]}')
    elif message['action'] == 'response':
        with consensus_lock:
            consensus_responses[message['responder_id']] = message['response']
            check_consensus_reached()
            
def get_file_version(file_name):
    """
    Get the version of the file based on its last modification time.
    """
    file_path = os.path.join(file_dir, file_name)
    if os.path.exists(file_path):
        # Use the last modified time as the version
        return os.path.getmtime(file_path)
    else:
        return None 
    
def evaluate_consensus_proposal(proposal):
    # Implement logic to evaluate the proposal, e.g., check file version
    local_version = get_file_version(proposal['file_name'])
    if local_version >= proposal['version']:
        return 'ACK'
    else:
        return 'NACK'

def check_consensus_reached():
    global consensus_result
    ack_count = sum(1 for response in consensus_responses.values() if response == 'ACK')
    if ack_count > len(known_nodes) / 2:
        consensus_result = 'Consensus Reached'
    elif len(consensus_responses) == len(known_nodes):
        consensus_result = 'Consensus Not Reached'





# RabbitMQ and other inter-node communication setup


def setup_rabbitmq(node_id):

    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbitmq_config['host'], port=rabbitmq_config['port']))
    channel = connection.channel()

    # Declare all the queues
    print(f"Setting up RabbitMQ for node {node_id}")
    channel.queue_declare(queue=f'file_operations_queue_{node_id}')
    channel.queue_declare(queue='election_queue')
    channel.queue_declare(queue=f'leader_announcement_queue_{node_id}')
    channel.queue_declare(queue=f'file_list_request_queue_{node_id}')
    channel.queue_declare(queue='file_list_response_queue')
    channel.queue_declare(queue=f'heartbeat_queue_{node_id}')
    channel.queue_declare(queue=f'storage_info_request_queue_{node_id}')

    channel.queue_declare(queue=f'storage_info_response_queue_{node_id}')
    
    channel.queue_declare(queue=f'file_query_queue_{node_id}')
    channel.queue_declare(queue=f'file_retrieve_queue_{node_id}')

    # Declare consensus queue
    consensus_queue = f'consensus_queue_{node_id}'
    channel.queue_declare(queue=consensus_queue)
    channel.queue_declare(queue=f'file_data_queue_{self_node_id}')
    
    def callback(ch, method, properties, body):
        on_message_received(ch, method, properties, body, node_id)
    # Setup consumer for consensus messages
    channel.basic_consume(queue=consensus_queue, on_message_callback=handle_consensus_message, auto_ack=True)

    channel.basic_consume(queue=f'file_query_queue_{node_id}', on_message_callback=handle_file_query_message, auto_ack=True)
    channel.basic_consume(queue=f'file_retrieve_queue_{node_id}', on_message_callback=handle_file_retrieve_message, auto_ack=True)
    channel.basic_consume(queue=f'file_data_queue_{self_node_id}', on_message_callback=handle_file_data_message, auto_ack=True)
    setup_discovery_queue(self_node_id)
    # Consume from queues
    channel.basic_consume(
        queue=f'storage_info_request_queue_{node_id}', on_message_callback=callback, auto_ack=True)

    channel.basic_consume(
        queue=f'file_operations_queue_{node_id}', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue='election_queue',
                          on_message_callback=callback, auto_ack=True)
    channel.basic_consume(
        queue=f'leader_announcement_queue_{node_id}', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(
        queue=f'file_list_request_queue_{node_id}', on_message_callback=callback, auto_ack=True)
    if is_coordinator():
        # Coordinator consumes file list responses
        channel.basic_consume(queue='file_list_response_queue',
                              on_message_callback=callback, auto_ack=True)

    channel.basic_consume(
        queue=f'heartbeat_queue_{node_id}', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(
        queue=f'storage_info_response_queue_{node_id}', on_message_callback=callback, auto_ack=True)

    channel.start_consuming()
    # Start Bully election and consuming messages
    # start_bully_election(node_id)
    print(f"Server {node_id} running on port {port}")
    channel.start_consuming()


heartbeat_interval = 180  # seconds
heartbeat_timeout = 180*3  # seconds
last_heartbeat = {}


def send_heartbeat():
    # Send a heartbeat message
    global leader_node_id
    if leader_node_id is None:
        return  # Leader not defined yet

    heartbeat_message = json.dumps(
        {'node_id': self_node_id, 'type': 'heartbeat', 'sender': 'participant'})
    for server in participant_servers:
        if server['node_id'] != self_node_id:
            send_message_to_queue(
                heartbeat_message, f"heartbeat_queue_{server['node_id']}")


def start_participant_heartbeat():
    while True:
        send_heartbeat()
        time.sleep(heartbeat_interval)


def start_coordinator_heartbeat():
    while True:
        send_coordinator_heartbeat()
        time.sleep(heartbeat_interval)


# consistency and
def send_coordinator_heartbeat():
    # Coordinator sends a heartbeat message to all participant nodes
    heartbeat_message = json.dumps(
        {'node_id': self_node_id, 'type': 'heartbeat', 'sender': 'coordinator'})
    for server in participant_servers:
        if server['node_id'] != self_node_id:
            send_message_to_queue(
                heartbeat_message, f"heartbeat_queue_{server['node_id']}")

def check_heartbeats(leader_node_id):
    global last_heartbeat, participant_servers
    current_time = time.time()
    failed_nodes = []
    # Check if any node missed sending heartbeats
    current_time = time.time()
    for node_id, last_time in last_heartbeat.items():
        if current_time - last_time > heartbeat_timeout:
            print(
                f"Node {node_id} is not responding. Last heartbeat was at {last_time}")
            # Handle node failure, e.g., reassign tasks, replicate data, etc.
            failed_nodes.append(node_id)

    # Handle each failed node
    for node_id in failed_nodes:
        handle_node_failure(node_id)
        
def handle_node_failure(node_id):
    global participant_servers, leader_node_id
    # Remove the failed node from the list of participant servers
    participant_servers = [server for server in participant_servers if server['node_id'] != node_id]

    # If the failed node was the leader, initiate a re-election
    if node_id == leader_node_id:
        print(f"Leader node {node_id} failed. Initiating re-election.")
        start_bully_election(self_node_id)

def handle_heartbeat(message):
    message_data = json.loads(message)
    node_id = message_data['node_id']
    sender_type = message_data['sender']

    if sender_type == 'coordinator':
        # Handle heartbeat received from the coordinator
        # Update some status or timestamp as needed
        print(f"Heartbeat received from coordinator node {node_id}")
    elif sender_type == 'participant':
        # Handle heartbeat received from a participant
        # This would typically be on the coordinator
        last_heartbeat[node_id] = time.time()
        print(f"Heartbeat received from participant node {node_id}")


def handle_file_operation(message, node_id):
    # Parse the message
    message_data = json.loads(message)
    action = message_data['action']
    file_name = message_data['file_name']
    file_content = message_data.get('file_content', '')
    
    message_data = json.loads(message)
    action = message_data['action']
    file_name = message_data['file_name']
    file_content = message_data.get('file_content', '')

    if action == 'upload':
        # Save or update the file
        with open(os.path.join(file_dir, file_name), 'w') as file:
            file.write(file_content)
        replicate_file(file_name, file_content, node_id)
        new_version = get_file_version(file_name)
    elif action == 'replicate':
        # The message is already a dict, no need to parse again
        handle_replication_request(message_data)


def on_message_received(ch, method, properties, body, node_id):
    print(
        f"Message received on node {node_id}, queue {method.routing_key}: {body.decode()}")

    message = body.decode()
    global leader_node_id
    global leader_node_id, file_list_responses
    message = body.decode()

    if method.routing_key.startswith('file_operations_queue'):

        handle_file_operation(message, node_id)
    if method.routing_key == 'election_queue':
        sender_node_id = int(body.decode().split()[-1])
        handle_election_message(sender_node_id)
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    elif method.routing_key.startswith('leader_announcement_queue'):
        leader_node_id = int(body.decode().split()[-1])
        print(f"Received leader announcement: {leader_node_id}")
        if leader_node_id == node_id:
            # Request proposed consensus values from other nodes
            request_consensus_values_from_nodes(node_id)

    elif method.routing_key.startswith('consensus_queue'):
        # Handle proposed consensus value from another node
        consensus_value = body.decode().split()[-1]
        print(
            f"Received proposed consensus value: {consensus_value} from another node")
        consensus_proposed_values.append(consensus_value)
    if method.routing_key == 'file_list_response_queue' and is_coordinator():
        # Handle file list response only on the coordinator
        response_data = json.loads(body.decode())
        with file_list_responses_lock:
            file_list_responses[response_data['node_id']] = response_data
            print(
                f"Received file list response from node {response_data['node_id']}")

    if method.routing_key.startswith('file_list_request_queue'):
        # Handle file list request on all nodes except coordinator
        if not is_coordinator():
            handle_file_list_request(node_id)
    if method.routing_key.startswith('heartbeat_queue'):
        handle_heartbeat(message)

    if method.routing_key == f'storage_info_request_queue_{node_id}':
        handle_storage_info_request()
    if method.routing_key == f'storage_info_response_queue_{node_id}':
        handle_storage_info_message(body)


def on_coordinator_message_received(ch, method, properties, body):
    message = json.loads(body.decode())
    node_id = message['node_id']
    storage_info = message['storage_info']

    node_storage_info[node_id] = storage_info


def start_heartbeat(node_id):
    while True:
        send_heartbeat(node_id)
        time.sleep(heartbeat_interval)  # Heartbeat interval in seconds


def start_server(node_config, config):
    global self_node_id, file_dir, rabbitmq_config, participant_servers

    if node_config is None:
        print("No matching configuration found for this server's IP address.")
        return

    self_node_id = node_config['node_id']
    participant_servers = config['participant_servers']
    rabbitmq_config = config['rabbitmq']
    file_dir = f"files_node_{self_node_id}"

    if not os.path.exists(file_dir):
        os.makedirs(file_dir)

    # Use the provided port number from the command-line argument
    flask_thread = threading.Thread(
        target=lambda: app.run(host='0.0.0.0', port=args.port))
    flask_thread.start()

    # Setup RabbitMQ for receiving replication requests and other inter-node communication
    rabbitmq_thread = threading.Thread(
        target=setup_rabbitmq, args=(self_node_id,))
    rabbitmq_thread.start()
    start_bully_election(self_node_id)

    if is_coordinator():
        heartbeat_thread = threading.Thread(target=start_coordinator_heartbeat)
    else:
        heartbeat_thread = threading.Thread(target=start_participant_heartbeat)

    heartbeat_thread.start()
    
    broadcast_discovery()


# Load configurations and start the server
config = load_config()
ip_address = get_ip_address()
self_config = find_self_config(config, ip_address)
print(self_config)
start_server(self_config, config)
