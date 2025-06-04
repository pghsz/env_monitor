import json
import time
import logging
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import pubsub_v1

# Import sensor modules
from temp_sensor import get_cpu_temperature
from system_metrics import get_system_metrics

# Load environment variables
load_dotenv()

# Set up logging with absolute path
LOG_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(os.path.dirname(LOG_DIR), "pubsub_client.log")

# Create a custom logger
logger = logging.getLogger("pubsub_client")
logger.setLevel(logging.INFO)

# Create handlers
file_handler = logging.FileHandler(LOG_FILE)
console_handler = logging.StreamHandler()

# Create formatters and add to handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Log the actual path being used
logger.info(f"Logging to file: {LOG_FILE}")

# GCP Settings
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_ID = os.getenv("GCP_TOPIC_ID")
SERVICE_ACCOUNT_KEY = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
SAMPLE_INTERVAL = int(os.getenv("SAMPLE_INTERVAL", 60))  # Default: 60 seconds
DEVICE_ID = os.getenv("DEVICE_ID", "raspberry_pi_001")

# Set environment variable for Google credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY

logger.info(f"Using service account key: {SERVICE_ACCOUNT_KEY}")

def get_env_monitoring_data():
    """Collect all available environmental data"""
    # Get current timestamp
    timestamp = datetime.now().isoformat()
    
    # Get CPU temperature
    cpu_temp = get_cpu_temperature()
    
    # Get system metrics
    system_metrics = get_system_metrics()
    
    # Build the payload
    data = {
        "device_id": DEVICE_ID,
        "timestamp": timestamp,
        "cpu_temperature": cpu_temp,
        "system_metrics": system_metrics,
        "version": "1.0.0"
    }
    
    # Add additional metadata (from original MQTT client)
    data["metadata"] = {
        "sample_interval_seconds": SAMPLE_INTERVAL,
        "available_sensors": ["cpu_temperature", "system_metrics"]
    }
    
    return data

def validate_data(data):
    """Basic validation of collected data"""
    # Check if CPU temperature is in a reasonable range
    if data.get("cpu_temperature") is not None:
        cpu_temp = data["cpu_temperature"]
        if cpu_temp < 0 or cpu_temp > 100:
            logger.warning(f"CPU temperature outside expected range: {cpu_temp}°C")
            data["warnings"] = data.get("warnings", []) + ["Abnormal CPU temperature"]
    
    # Check if system metrics are present
    if "system_metrics" not in data or not isinstance(data["system_metrics"], dict):
        logger.warning("System metrics missing or invalid")
        data["warnings"] = data.get("warnings", []) + ["Missing system metrics"]
    
    return data

def publish_data_to_pubsub():
    """Collect, validate and publish data to GCP Pub/Sub"""
    try:
        # Create a publisher client
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
        
        # Collect data
        data = get_env_monitoring_data()
        
        # Validate data (from original MQTT client)
        data = validate_data(data)
        
        # Convert to JSON string
        data_str = json.dumps(data)
        
        # Encode as bytes
        data_bytes = data_str.encode("utf-8")
        
        # Publish message
        future = publisher.publish(topic_path, data=data_bytes)
        message_id = future.result()
        
        logger.info(f"Published message ID: {message_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error publishing to Pub/Sub: {e}")
        return False

def run_once():
    """Run data collection and Pub/Sub publishing once (for testing)"""
    try:
        # Publish data once
        logger.info("Publishing environmental data (test run)")
        success = publish_data_to_pubsub()
        
        if success:
            logger.info("Data published successfully")
        else:
            logger.error("Failed to publish data")
        
    except Exception as e:
        logger.error(f"Error in test run: {e}")

def run_continuous():
    """Run continuous data collection and Pub/Sub publishing"""
    # Track reconnection attempts (from original MQTT client)
    reconnect_count = 0
    max_reconnect_attempts = 10
    reconnect_delay = 5  # seconds
    
    try:
        logger.info(f"Starting environmental monitoring with Pub/Sub (interval: {SAMPLE_INTERVAL}s)")
        
        while True:
            try:
                # Publish data
                logger.info(f"Collecting and publishing environmental data")
                success = publish_data_to_pubsub()
                
                if not success:
                    logger.warning("Failed to publish data to Pub/Sub")
                    
                    # Increment reconnection counter
                    reconnect_count += 1
                    
                    if reconnect_count > max_reconnect_attempts:
                        logger.error(f"Maximum reconnection attempts ({max_reconnect_attempts}) reached")
                        raise Exception("Maximum reconnection attempts reached")
                        
                    # Wait with backoff strategy
                    wait_time = reconnect_delay * reconnect_count
                    logger.warning(f"Retrying in {wait_time}s (attempt {reconnect_count}/{max_reconnect_attempts})")
                    time.sleep(wait_time)
                    continue
                
                # Reset reconnection counter on success
                reconnect_count = 0
                
                # Wait for next collection interval
                time.sleep(SAMPLE_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error in continuous monitoring: {e}")
                
                # Increment reconnection counter
                reconnect_count += 1
                
                if reconnect_count > max_reconnect_attempts:
                    logger.error(f"Maximum reconnection attempts ({max_reconnect_attempts}) reached")
                    raise
                    
                # Wait with backoff strategy
                wait_time = reconnect_delay * reconnect_count
                logger.warning(f"Retrying in {wait_time}s (attempt {reconnect_count}/{max_reconnect_attempts})")
                time.sleep(wait_time)
    
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Unrecoverable error: {e}")
    finally:
        logger.info("Program terminated")

if __name__ == "__main__":
    # Log current working directory and script location
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"Script location: {os.path.abspath(__file__)}")
    
    # Check for command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Run once for testing
        run_once()
    else:
        # Run continuous monitoring
        run_continuous()
