import random
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
import json

class RaftNode:
    def __init__(self, node_id, num_nodes):
        self.node_id = node_id
        self.num_nodes = num_nodes
        self.current_term = 0
        self.voted_for = None
        self.state = 'follower'
        self.leader_id = None
        self.election_timeout = random.randint(150, 300) / 1000.0
        self.heartbeat_interval = 0.1
    
    def follower(self):
        while self.state == 'follower':
            # Implement follower logic here
            # Listen for incoming messages and respond accordingly
            # Monitor for leader heartbeats to reset the election timeout
            # Transition to candidate state if the election timeout is reached

            # Example: Pseudo code to listen for incoming messages
            message = self.receive_message()
            if message['type'] == 'AppendEntries':
                self.handle_append_entries(message)
            elif message['type'] == 'RequestVote':
                self.handle_vote_request(message)
            elif message['type'] == 'Heartbeat':
                self.handle_heartbeat(message)

            # Check for leader heartbeats to reset the election timeout
            if time.time() - self.last_heartbeat_received > self.election_timeout:
                # Transition to candidate state and start a new election
                self.state = 'candidate'
                self.start_election()

            # Sleep for a short period to avoid busy-waiting
            time.sleep(0.05)

    def candidate(self):
        while self.state == 'candidate':
            # Implement candidate logic here
            # Request votes from other nodes, manage vote counting, etc.

            # Example: Pseudo code to request votes
            self.request_votes()

            # Initialize a vote counter
            received_votes = 1  # Vote for self

            # Check if received enough votes to become a leader
            if received_votes > self.num_nodes // 2:
                self.state = 'leader'
                self.become_leader()

            time.sleep(0.05)

    def leader(self):
        while self.state == 'leader':
            # Implement leader logic here
            # Send periodic heartbeats to followers, manage log replication, etc.

            # Example: Pseudo code to send heartbeats
            self.send_heartbeats()

            # Manage log replication and other leader responsibilities

            time.sleep(self.heartbeat_interval)

    def receive_message(self):
        # Simulate receiving messages from other nodes
        # In a real implementation, you would use a network transport to receive messages.
        # You can customize this part based on your project's requirements
        message = {
            'type': random.choice(['AppendEntries', 'RequestVote', 'Heartbeat']),
            'term': self.current_term,
            'sender': random.randint(0, self.num_nodes - 1),
        }
        return message
        pass

    def handle_append_entries(self, message):
        # Handle AppendEntries RPC from the leader
        if message['term'] < self.current_term:
            # Reject the request
            response = {'term': self.current_term, 'success': False}
            self.send_response(message['sender'], response)
            return
        # Process the leader's entries and update the log
        # Respond with success or failure
        response = {'term': self.current_term, 'success': True}
        self.send_response(message['sender'], response)
        pass

    def handle_vote_request(self, message):
        # Handle RequestVote RPC from a candidate
        if message['term'] < self.current_term:
            # Reject the request
            response = {'term': self.current_term, 'voteGranted': False}
            self.send_response(message['sender'], response)
            return
        # Check candidate's log for consistency and grant or deny the vote
        if self.voted_for is None:
            self.voted_for = message['candidateId']
            response = {'term': self.current_term, 'voteGranted': True}
            self.send_response(message['sender'], response)
        else:
            response = {'term': self.current_term, 'voteGranted': False}
            self.send_response(message['sender'], response)
        pass

    def handle_heartbeat(self, message):
        # Handle leader's heartbeat
        if message['term'] >= self.current_term:
            self.state = 'follower'
            self.voted_for = None
            self.last_heartbeat_received = time.time()
        pass

    
    def send_request_vote_request(self, target_node, request):
        # Simulate sending RequestVote RPC to the target_node and receiving a response
        # In a real implementation, you would use a network transport to send the RPC.

        # Simulate a response indicating whether the vote is granted
        # You can customize this part based on your project's requirements
        vote_granted = random.choice([True, False])

        response = {
            'term': self.current_term,  # Include the current term in the response
            'voteGranted': vote_granted,  # Indicate whether the vote is granted
        }

        return response


    def request_votes(self):
        # Send RequestVote RPCs to other nodes
        # Send RequestVote RPCs to other nodes
        # Pseudo code for sending RequestVote to other nodes
        request = {'type': 'RequestVote', 'term': self.current_term, 'candidateId': self.node_id}
        for node in range(self.num_nodes):
            if node != self.node_id:
                response = self.send_request_vote_request(node, request)
                if response['voteGranted']:
                    received_votes += 1
        pass

    def become_leader(self):
        # Perform tasks when becoming a leader
        pass

    def send_heartbeats(self):
        # Send periodic heartbeats to followers
        pass


    # Implement Raft functionalities like leader election, heartbeat, etc.
    def start(self):
        while True:
            if self.state == 'follower':
                self.follower()
            elif self.state == 'candidate':
                self.candidate()
            elif self.state == 'leader':
                self.leader()
            else:
                raise Exception('Invalid state')
        
        pass

    def send_response(self, target_node, response):
        # Simulate sending a response to a node
        # In a real implementation, you would use a network transport to send the response.
        # Simulate sending a response to a node
        # In a real implementation, you would use a network transport to send the response.
        
        # Simulate a response delay (in seconds)
        response_delay = random.uniform(0.01, 0.05)
        
        # Print a message to simulate response
        print(f"Node {self.node_id} sends a response to Node {target_node} with a delay of {response_delay} seconds.")
        
        # Simulate the response delay
        time.sleep(response_delay)
        
        # In a real implementation, you would send the response to the target node over the network.
        # You can customize this part based on your project's requirements.
        

        pass



class RaftHTTPHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        data = json.loads(post_data)

        if self.path == '/register_broker':
            # Handle broker registration
            # Add the logic for RegisterBrokerRecord here
            pass
        elif self.path == '/register_producer':
            # Handle producer registration
            # Add the logic for RegisterProducerRecord here
            pass
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write("Not Found".encode('utf-8'))

def raft_node_thread(node_id, num_nodes):
    raft_node = RaftNode(node_id, num_nodes)
    raft_node.start()

def http_server_thread(port):
    server = HTTPServer(('localhost', port), RaftHTTPHandler)
    server.serve_forever()

def main():
    num_nodes = 5
    nodes = []

    for node_id in range(num_nodes):
        t = threading.Thread(target=raft_node_thread, args=(node_id, num_nodes))
        t.daemon = True
        t.start()
        nodes.append(t)

    http_server_port = 8080
    http_server = threading.Thread(target=http_server_thread, args=(http_server_port,))
    http_server.daemon = True
    http_server.start()

    try:
        for node in nodes:
            node.join()
        http_server.join()
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()
