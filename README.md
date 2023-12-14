
# Distributed File Sharing System Project

## Project Title
Distributed File Sharing System

https://github.com/safiyesarac/distributed_file_sharing_sarac_zaman/assets/66124396/abb8b2dd-2a28-4565-94e8-d3faa79ff69c



## Description
This project involves the design and implementation of a distributed file-sharing system as part of the Distributed Systems course project for 2023. It focuses on creating a complex-enough distributed system with a running prototype on at least three separate nodes.

<video src='DS_Demo.mp4' width=180> </video>

## Features
- Shared distributed state across nodes.
- Communication via Internet protocol-based message exchange.
- File replication and synchronization across nodes.
- Fault tolerance and consensus mechanisms.
- Node discovery and dynamic leader election.
- Basic file upload and download functionalities.

## System Requirements
- Python 3.x
- Flask for creating web server
- RabbitMQ for message queue management
- Additional Python libraries: `pika`, `hashlib`, `json`, `socket`, `threading`, `time`, `base64`, `shutil`, `os`

## Installation and Setup
1. Clone the repository.
2. Install the required Python packages: `pip install flask pika`.
3. Set up RabbitMQ on the desired host machine.
4. Update the `config.json` file with the relevant node and RabbitMQ configuration.
5. Start the server on each node using the command: `python main.py --port [PORT_NUMBER]`.

## Usage
- Upload files to the coordinator node via the Flask web interface.
- Download files from any node where the file is replicated.
- View the list of files available on the network.
- The system automatically handles file replication and synchronization across nodes.

## Contributing
- Fork the repository.
- Create a new branch for your feature (`git checkout -b feature/AmazingFeature`).
- Commit your changes (`git commit -m 'Add some AmazingFeature'`).
- Push to the branch (`git push origin feature/AmazingFeature`).
- Open a pull request.

