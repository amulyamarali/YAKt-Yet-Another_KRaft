import requests
import json

# Set the URL of your Flask server
url = 'http://localhost:5000/api/broker-mgmt'  # Replace with your actual endpoint


# Data to be sent in JSON format
data = {
        "timestamp": 1700490376,
}

 
# Set the headers to indicate that we're sending JSON
headers = {'Content-Type': 'application/json'}

# Send the POST request with JSON data
response = requests.post(url, data=json.dumps(data), headers=headers)

# Print the response from the server
print("Response from server:")
print(response.status_code)  # HTTP status code
print(response.text)  # Response body
