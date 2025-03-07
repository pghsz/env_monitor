import psutil
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("system_metrics.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("system_metrics")

def get_system_metrics():
    """Collect system metrics from Raspberry Pi"""
    try:
        # CPU usage (percentage)
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Memory usage
        memory = psutil.virtual_memory()
        memory_used_percent = memory.percent
        
        # Disk usage for root partition
        disk = psutil.disk_usage('/')
        disk_used_percent = disk.percent
        
        # Network stats
        net_io = psutil.net_io_counters()
        bytes_sent = net_io.bytes_sent
        bytes_recv = net_io.bytes_recv
        
        return {
            "cpu_percent": round(cpu_percent, 1),
            "memory_percent": round(memory_used_percent, 1),
            "disk_percent": round(disk_used_percent, 1),
            "network": {
                "bytes_sent": bytes_sent,
                "bytes_recv": bytes_recv
            }
        }
        
    except Exception as e:
        logger.error(f"Error collecting system metrics: {e}")
        return {
            "status": "error",
            "message": str(e)
        }

if __name__ == "__main__":
    # Test the function
    metrics = get_system_metrics()
    print(f"CPU Usage: {metrics.get('cpu_percent')}%")
    print(f"Memory Usage: {metrics.get('memory_percent')}%")
    print(f"Disk Usage: {metrics.get('disk_percent')}%")
