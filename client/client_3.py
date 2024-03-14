import requests
import json

# Set the URL of your Flask server
url = 'http://localhost:5000/api/partition-record'  # Replace with your actual endpoint

# Data to be sent in JSON format
data = {
    "partitionId": 0,
    "topicUUID": 0,
    "replicas": [0,1,2], #brokerId replicas of the partition
    "isr": [0,1,2],
    "leader": 0,
    "epoch": 0
}

 
# Set the headers to indicate that we're sending JSON
headers = {'Content-Type': 'application/json'}

# Send the POST request with JSON data
response = requests.post(url, data=json.dumps(data), headers=headers)

# Print the response from the server
print("Response from server:")
print(response.status_code)  # HTTP status code
print(response.text)  # Response body
