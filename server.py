import base64
import threading
import json
import pika
import os
import flask
from hashlib import sha256
from flask import Flask, request, jsonify
import socket
from threading import Thread, Lock

import argparse
# ... other imports ...

app = Flask(__name__)

# Setup argument parser
parser = argparse.ArgumentParser(description='Start a server for a distributed file sharing system.')
parser.add_argument('-p', '--port', type=int, help='Port number to run the Flask app on', required=True)
args = parser.parse_args()

# Load the configuration file
def load_config():
    with open('config.json', 'r') as config_file:
        return json.load(config_file)
    

def get_ip_address():
    return socket.gethostbyname(socket.gethostname())

# Find the configuration of this server using IP address
def find_self_config(all_configs, ip_address):
    print(all_configs,ip_address)
    for config in all_configs['participant_servers']:
        print("----------------------",config['ip'])
        if config['ip'] == ip_address and config['port']==args.port:
            print("correct", config)
            return config
    return None
file_list_responses = {}
file_list_responses_lock = Lock()
file_list_request_timeout = 10  # seconds

def request_file_list_from_nodes(node_id):
    # Clear previous responses
    global file_list_responses
    file_list_responses = {}

    # Request a list of files from all other nodes
    for server in participant_servers:
        if server['node_id'] != node_id:
            send_message_to_queue("Request file list", f"file_list_request_queue_{server['node_id']}")

def handle_file_list_request(node_id):
    # Handle request to send list of files
    files = os.listdir(f'files_node_{node_id}')
    file_list_message = json.dumps({'node_id': node_id, 'files': files})
    send_message_to_queue(file_list_message, 'file_list_response_queue')

def aggregate_file_lists():
    with file_list_responses_lock:
        return {node_id: data['files'] for node_id, data in file_list_responses.items()}

# Calculate SHA-256 hash of a file
def calculate_file_hash(file_path):
    with open(file_path, 'rb') as f:
        return sha256(f.read()).hexdigest()

# Function to send message to a specific RabbitMQ queue
def send_message_to_queue(message, queue_name):
    print(f"Sending message to {queue_name}: {message}")
    global rabbitmq_config
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_config['host'], port=rabbitmq_config['port']))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    connection.close()

# Replicate file to other nodes
def replicate_file(file_name, content):
    # Encode the binary content to base64 string
    base64_content = base64.b64encode(content).decode('utf-8')
    file_hash = calculate_file_hash(os.path.join(file_dir, file_name))
    
    replication_message = json.dumps({
        'action': 'replicate', 
        'file_name': file_name, 
        'content': base64_content,  # Use the base64 encoded content
        'hash': file_hash
    })

    # Send the replication message to other nodes
    for server in participant_servers:
        if server['node_id'] != self_node_id:
            send_message_to_queue(replication_message, f"file_operations_queue_{server['node_id']}")



# Flask route for uploading files
@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    filename = file.filename
    filepath = os.path.join(file_dir, filename)
    file.save(filepath)
    with open(filepath, 'rb') as f:
        content = f.read()
    threading.Thread(target=replicate_file, args=(filename, content)).start()
    return jsonify({"message": "File uploaded successfully"}), 200

# Flask route for downloading files
@app.route('/download', methods=['GET'])
def download_file():
    filename = request.args.get('filename')
    filepath = os.path.join(file_dir, filename)
    if os.path.exists(filepath):
        return flask.send_file(filepath)
    else:
        return jsonify({"message": "File not found"}), 404

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
    file_path = os.path.join(file_dir, data['file_name'])

    # Decode the content from base64
    content = base64.b64decode(data['content'])

    with open(file_path, 'wb') as f:
        f.write(content)

    if calculate_file_hash(file_path) != data['hash']:
        print(f"Consistency error in replicating {data['file_name']}")

leader_node_id = None

def send_election_message(to_node_id):
    print("election message sent")
    election_message = json.dumps({'type': 'election', 'from_node_id': self_node_id})
    send_message_to_queue(election_message, f"election_queue_{to_node_id}")
leader_node_id = None

def start_bully_election(current_node_id):
    print(f"Starting Bully election from node {current_node_id}")
    global leader_node_id
    higher_nodes = [server for server in participant_servers if server['node_id'] > current_node_id]

    if not higher_nodes:
        leader_node_id = current_node_id
        announce_leader(current_node_id)
        return

    for node in higher_nodes:
        send_election_message(node['node_id'])
        # Wait for response or timeout
        # ...

def handle_election_message(from_node_id):
    global leader_node_id
    if self_node_id > from_node_id:
        send_election_message(from_node_id)
        start_bully_election(self_node_id)

def announce_leader(node_id):
    for server in participant_servers:
        if server['node_id'] != node_id:
            send_message_to_queue(f"Leader {node_id}", f"leader_announcement_queue_{server['node_id']}")


# RabbitMQ and other inter-node communication setup
def setup_rabbitmq(node_id):
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_config['host'], port=rabbitmq_config['port']))
    channel = connection.channel()

    # Declare all the queues
    print(f"Setting up RabbitMQ for node {node_id}")
    channel.queue_declare(queue=f'file_operations_queue_{node_id}')
    print(f"Queue declared: file_operations_queue_{node_id}")
    channel.queue_declare(queue='election_queue')
    channel.queue_declare(queue=f'leader_announcement_queue_{node_id}')
    channel.queue_declare(queue=f'file_list_request_queue_{node_id}')
    channel.queue_declare(queue='file_list_response_queue')
    channel.queue_declare(queue=f'heartbeat_queue_{node_id}')

    
  
    def callback(ch, method, properties, body):
        on_message_received(ch, method, properties, body, node_id)

    # Consume from queues
    channel.basic_consume(queue=f'file_operations_queue_{node_id}', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue='election_queue', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue=f'leader_announcement_queue_{node_id}', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue=f'file_list_request_queue_{node_id}', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue='file_list_response_queue', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue=f'heartbeat_queue_{node_id}', on_message_callback=callback, auto_ack=True)

    channel.start_consuming()
    # Start Bully election and consuming messages
    #start_bully_election(node_id)
    print(f"Server {node_id} running on port {port}")
    channel.start_consuming()
   

heartbeat_interval = 5  # seconds
heartbeat_timeout = 15  # seconds
last_heartbeat = {}
import time
def send_heartbeat(node_id):
    # Send a heartbeat message to all nodes
    heartbeat_message = json.dumps({'node_id': node_id, 'type': 'heartbeat'})
    for server in participant_servers:
        if server['node_id'] != node_id:
            send_message_to_queue(heartbeat_message, f"heartbeat_queue_{server['node_id']}")
#consistency and 

def check_heartbeats(leader_node_id):
    # Check if any node missed sending heartbeats
    current_time = time.time()
    for node_id, last_time in last_heartbeat.items():
        if current_time - last_time > heartbeat_timeout:
            print(f"Node {node_id} is not responding. Last heartbeat was at {last_time}")
            # Handle node failure, e.g., reassign tasks, replicate data, etc.

def handle_heartbeat(message):
    # Update the last heartbeat time for the node
    message_data = json.loads(message)
    node_id = message_data['node_id']
    last_heartbeat[node_id] = time.time()

def start_heartbeat(node_id):
    while True:
        send_heartbeat(node_id)
        time.sleep(heartbeat_interval)
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
    elif action == 'replicate':
        # The message is already a dict, no need to parse again
        handle_replication_request(message_data)
        

        
def process_election_message(from_node_id, current_node_id):
    if from_node_id < current_node_id:
        # Respond to the election message indicating a higher node is active
        send_election_message(from_node_id)
        # Start a new election
        start_bully_election(current_node_id)

def announce_leader(node_id):
    # Announce the current node as the leader to all nodes
    for server in participant_servers:
        if server['node_id'] != node_id:
            send_message_to_queue(f"Leader {node_id}", f"leader_announcement_queue_{server['node_id']}")

def on_message_received(ch, method, properties, body, node_id):
    print(f"Message received on node {node_id}, queue {method.routing_key}: {body.decode()}")
    
    message = body.decode()
    global leader_node_id
    global leader_node_id, file_list_responses
    message = body.decode()

    if method.routing_key.startswith('file_operations_queue'):
        
        handle_file_operation(message, node_id)
    elif method.routing_key == 'election_queue':
        sender_node_id = int(message.split()[-1])
        process_election_message(sender_node_id, node_id)
    elif method.routing_key.startswith('leader_announcement_queue'):
        leader_node_id = int(message.split()[-1])
        if leader_node_id == node_id:
            request_file_list_from_nodes(node_id)
    elif method.routing_key.startswith('file_list_request_queue'):
        handle_file_list_request(node_id)
    elif method.routing_key == 'file_list_response_queue':
        if leader_node_id == node_id:
            response_data = json.loads(message)
            with file_list_responses_lock:
                file_list_responses[response_data['node_id']] = response_data
    if method.routing_key.startswith('heartbeat_queue'):
        handle_heartbeat(message)
    
            

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
    flask_thread = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=args.port))
    flask_thread.start()

    # Setup RabbitMQ for receiving replication requests and other inter-node communication
    rabbitmq_thread = threading.Thread(target=setup_rabbitmq, args=(self_node_id,))
    rabbitmq_thread.start()
    start_bully_election(self_node_id)

# Load configurations and start the server
config = load_config()
ip_address = get_ip_address()
print(ip_address)
self_config = find_self_config(config, ip_address)
print("******",self_config)
start_server(self_config, config)
