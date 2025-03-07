import subprocess
import re
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("temp_sensor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("temp_sensor")

def get_cpu_temperature():
    """Read CPU temperature from Raspberry Pi"""
    try:
        # Execute vcgencmd to get temperature
        temp_output = subprocess.check_output(['vcgencmd', 'measure_temp']).decode('utf-8')
        
        # Extract temperature value using regex
        temp_value = re.search(r'temp=(.*?)\'C', temp_output)
        
        if temp_value:
            # Convert to float
            return float(temp_value.group(1))
        else:
            logger.error("Failed to parse temperature output")
            return None
            
    except Exception as e:
        logger.error(f"Error reading CPU temperature: {e}")
        return None

if __name__ == "__main__":
    # Test the function
    temp = get_cpu_temperature()
    print(f"CPU Temperature: {temp}°C")
