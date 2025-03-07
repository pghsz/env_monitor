import paho.mqtt.client as mqtt
import json
import time
import logging
import os
from datetime import datetime
from dotenv import load_dotenv

# Import sensor modules
from temp_sensor import get_cpu_temperature
from system_metrics import get_system_metrics

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("mqtt_client.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("mqtt_client")

# MQTT Settings
MQTT_BROKER = os.getenv("MQTT_BROKER", "test.mosquitto.org")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "env_monitor/data")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", f"env_monitor_{int(time.time())}")
SAMPLE_INTERVAL = int(os.getenv("SAMPLE_INTERVAL", 60))  # Default: 60 seconds

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    connection_responses = {
        0: "Connected successfully",
        1: "Incorrect protocol version",
        2: "Invalid client identifier",
        3: "Server unavailable",
        4: "Bad username or password",
        5: "Not authorized"
    }
    
    message = connection_responses.get(rc, f"Unknown error code: {rc}")
    
    if rc == 0:
        logger.info(f"Connected to MQTT broker at {MQTT_BROKER}: {message}")
    else:
        logger.error(f"Failed to connect to MQTT broker: {message}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning(f"Unexpected disconnection from MQTT broker, code: {rc}")
    else:
        logger.info("Disconnected from MQTT broker")

def on_publish(client, userdata, mid):
    logger.info(f"Message {mid} published successfully")

def on_log(client, userdata, level, buf):
    if level == mqtt.MQTT_LOG_ERR:
        logger.error(f"MQTT Error: {buf}")
    elif level == mqtt.MQTT_LOG_WARNING:
        logger.warning(f"MQTT Warning: {buf}")

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
        "device_id": os.getenv("DEVICE_ID", "raspberry_pi_monitor"),
        "timestamp": timestamp,
        "cpu_temperature": cpu_temp,
        "system_metrics": system_metrics,
        "version": "1.0.0"
    }
    
    # Add additional metadata
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

def publish_data(client):
    """Collect, validate and publish data to MQTT broker"""
    try:
        # Collect data
        data = get_env_monitoring_data()
        
        # Validate data
        data = validate_data(data)
        
        # Convert to JSON
        payload = json.dumps(data)
        
        # Publish to MQTT broker
        result = client.publish(
            topic=MQTT_TOPIC,
            payload=payload,
            qos=1,  # QoS 1 = At least once delivery
            retain=False
        )
        
        # Check publish result
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.error(f"Failed to publish message, error code: {result.rc}")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"Error publishing data: {e}")
        return False

def setup_mqtt_client():
    """Set up and configure MQTT client"""
    # Create MQTT client instance
    client = mqtt.Client(client_id=MQTT_CLIENT_ID, clean_session=True)
    
    # Set callbacks
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish
    client.on_log = on_log
    
    # Configure authentication if provided
    username = os.getenv("MQTT_USERNAME")
    password = os.getenv("MQTT_PASSWORD")
    if username and password:
        client.username_pw_set(username, password)
    
    # Configure TLS if enabled
    use_tls = os.getenv("MQTT_USE_TLS", "False").lower() == "true"
    if use_tls:
        client.tls_set()
    
    return client

def run_once():
    """Run data collection and MQTT publishing once (for testing)"""
    # Set up MQTT client
    client = setup_mqtt_client()
    
    try:
        # Connect to broker
        logger.info(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        client.loop_start()
        
        # Allow time for connection to establish
        time.sleep(1)
        
        # Publish data once
        logger.info("Publishing environmental data (test run)")
        success = publish_data(client)
        
        if success:
            logger.info("Data published successfully")
        else:
            logger.error("Failed to publish data")
        
        # Wait for message to be delivered
        time.sleep(2)
        
    except Exception as e:
        logger.error(f"Error in test run: {e}")
    finally:
        # Disconnect and clean up
        client.loop_stop()
        client.disconnect()
        logger.info("Test run completed")

def run_continuous():
    """Run continuous data collection and MQTT publishing"""
    # Set up MQTT client
    client = setup_mqtt_client()
    
    # Track reconnection attempts
    reconnect_count = 0
    max_reconnect_attempts = 10
    reconnect_delay = 5  # seconds
    
    try:
        while True:
            try:
                # Connect to broker
                if not client.is_connected():
                    logger.info(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
                    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
                    client.loop_start()
                    reconnect_count = 0
                
                # Publish data
                logger.info(f"Collecting and publishing environmental data (interval: {SAMPLE_INTERVAL}s)")
                publish_success = publish_data(client)
                
                if not publish_success:
                    logger.warning("Failed to publish data")
                
                # Wait for next collection interval
                time.sleep(SAMPLE_INTERVAL)
                
            except (ConnectionRefusedError, ConnectionError) as e:
                # Handle connection errors with backoff strategy
                reconnect_count += 1
                
                if reconnect_count > max_reconnect_attempts:
                    logger.error(f"Maximum reconnection attempts ({max_reconnect_attempts}) reached")
                    raise
                
                wait_time = reconnect_delay * reconnect_count
                logger.warning(f"Connection error: {e}. Attempting reconnect in {wait_time}s")
                time.sleep(wait_time)
                
                # Ensure client is stopped
                try:
                    client.loop_stop()
                    client.disconnect()
                except:
                    pass
                
                # Create new client instance
                client = setup_mqtt_client()
    
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Unrecoverable error: {e}")
    finally:
        # Clean up
        try:
            client.loop_stop()
            client.disconnect()
        except:
            pass
        logger.info("Program terminated")

if __name__ == "__main__":
    import sys
    
    # Check for command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Run once for testing
        run_once()
    else:
        # Run continuous monitoring
        run_continuous()
