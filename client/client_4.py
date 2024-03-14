import requests
import json

# Set the URL of your Flask server
url = 'http://localhost:5000/api/producer-id-record'  # Replace with your actual endpoint

# Data to be sent in JSON format
data = {
        "brokerId": 0, # id of requesting broker
		"epoch": 0, 
		"producerId": 0 # id of requested broker
}

 
# Set the headers to indicate that we're sending JSON
headers = {'Content-Type': 'application/json'}

# Send the POST request with JSON data
response = requests.post(url, data=json.dumps(data), headers=headers)

# Print the response from the server
print("Response from server:")
print(response.status_code)  # HTTP status code
print(response.text)  # Response body
