# YAKt Yet Another KRaft (Kafka Raft)



* KRaft is a event based, distributed metadata management system that was written to replace Zookeeper in the ecosystem of Kafka.
* It uses Raft as an underlying consensus algorithm to do log replication and manage consistency of state.
* It is a protocol introduced in Kafka for managing the replication and consensus of Kafka broker metadata.
* 

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Introduction

KRaft is a protocol and feature set introduced in the realm of distributed systems, particularly within the context of Apache Kafka. Apache Kafka is an open-source distributed streaming platform widely used for building real-time data pipelines and streaming applications. KRaft specifically addresses the management of metadata and the consensus protocol within Kafka clusters.

## Features

1. Durability:

    KRaft aims to enhance the durability of Kafka clusters. It provides a more robust mechanism for managing metadata and handling failures,
    ensuring data consistency and reliability.
   
2. Consensus Protocol:

    KRaft uses a consensus protocol, which is similar to the Raft consensus algorithm. This protocol ensures that all nodes in the Kafka cluster agree on the current state and leadership,            contributing to a more resilient and fault-tolerant system.
   
3. ZooKeeper Replacement:

    KRaft replaces the traditional reliance on Apache ZooKeeper for managing metadata and leader election. This reduction of external dependencies simplifies the overall Kafka architecture.

4. Scalability:

    KRaft is designed to support larger Kafka deployments, allowing clusters to handle a higher number of partitions and brokers. This scalability is crucial for organizations with growing data      processing needs.
   
5. Simplified Architecture:

    By eliminating the need for ZooKeeper, KRaft simplifies the architecture of Kafka clusters. This results in a more self-contained and streamlined system.
   
6. Leader Election:

    KRaft manages leader election for partitions through the consensus protocol. This ensures that there is a single leader for each partition, facilitating efficient data processing.

7. Incremental Migration:

    Existing Kafka users can migrate to KRaft incrementally. The transition involves upgrading brokers and updating configurations without requiring a complete overhaul of the existing Kafka         deployment.

8. Improved Recovery:

    KRaft is designed to improve recovery processes in the event of node failures or other disruptions. The consensus protocol helps maintain data integrity during such scenarios.

9. Log Replication:
    
    KRaft ensures consistent metadata changes through fault-tolerant log replication across all cluster nodes.

10. Fault Tolerance:
    
    KRaft handles node failures gracefully, maintaining cluster operation with leader election and ensuring uninterrupted data consistency.


## Getting Started

1. client.py: A client script for sending JSON data to the Flask server. It uses the requests library to make HTTP requests to a specified URL.

2. flask_http_server.py: A Flask server that handles HTTP requests and maintains an in-memory data structure (metadata_store) for storing records.

3. http_server.py: A simple HTTP server using Python's built-in http.server module. It serves content on a specified port.

4. modified_raft.py: An extension or modification of a Raft consensus algorithm implementation. It includes classes and functions related to the Raft protocol, likely used for managing distributed consensus and metadata consistency.

5. start.py: This script initializes a Raft node with basic configurations such as node ID, number of nodes, and election timeouts. It may serve as an entry point to start a node in the YAKt system.

6. init.py: A standard Python initializer script, possibly used for package initialization. It references RaftNode from raft.py, indicating the use of Raft nodes in the project.

7. interface.py: This file includes definitions related to network communication, such as a Talker class, likely used for inter-node communication within the Raft cluster.

8. protocol.py: Defines various message types and directions, along with results related to Raft protocol messages like vote requests and responses.

### Prerequisites

- List any software or tools users need to install before using your project.

### Installation

Provide step-by-step instructions on how to install your project.

## Usage

Show examples of how to use your project. Provide code snippets and highlight important features.

## License



This project is licensed under the [MIT License](LICENSE).
