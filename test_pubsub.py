import os
from google.cloud import pubsub_v1
import json

# Print key information (without private key content)
try:
    with open('rpi-pubsub-key.json', 'r') as f:
        key_data = json.load(f)
        print(f"Key Information:")
        print(f"- Type: {key_data.get('type')}")
        print(f"- Project ID: {key_data.get('project_id')}")
        print(f"- Client Email: {key_data.get('client_email')}")
        
        # Use the project ID from the key file
        PROJECT_ID = key_data.get('project_id')
except Exception as e:
    print(f"Error reading key file: {e}")
    exit(1)

# Set credentials environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/marco/env_monitor/rpi-pubsub-key.json'

# Topic details
TOPIC_ID = 'env-monitor-data'

try:
    # Create publisher
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    
    # List available topics to confirm connection
    print(f"\nAttempting to list topics...")
    project_path = f"projects/{PROJECT_ID}"
    topics = list(publisher.list_topics(request={"project": project_path}))
    print(f"Available topics: {[t.name for t in topics]}")
    
    # Publish a test message
    print(f"\nAttempting to publish a message...")
    data = '{"device_id": "raspberry_pi_001", "temperature": 23.5, "humidity": 55}'.encode('utf-8')
    future = publisher.publish(topic_path, data=data)
    message_id = future.result()
    
    print(f'\nSUCCESS! Published message ID: {message_id}')
    print(f'Service account is working correctly')
except Exception as e:
    print(f'\nERROR: {e}')
    print('Service account test failed')
