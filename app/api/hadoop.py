import os
import pyhdfs
from datetime import datetime
from structlog import get_logger
from dotenv import load_dotenv

logger = get_logger()
load_dotenv()

# Create Hadoop Connection
fs = pyhdfs.HdfsClient(hosts=f"{os.getenv('HDFS_HOST')}:{os.getenv('HDFS_PORT')}", user_name={os.getenv('HDFS_USER')})

def get_list_hdfs_directory(path):
    """
        This function retrieves a list of files and folders along with their metadata from a Hadoop directory.
    """
    try:
        files = fs.list_status(path)

        return sorted(
            [
                {
                    "name": f["pathSuffix"],
                    "type": "Folder" if f["type"] == "DIRECTORY" else "File",
                    "size_kb": round(f.get("length", 0) / 1024, 2),
                    "mod_time": datetime.fromtimestamp(f["modificationTime"] / 1000).strftime("%d %b %Y, %H:%M %p")
                }
                for f in files
            ],
            key=lambda x: (x["type"] != "Folder", x["name"])  # Sort: folders first, then files
        )
    except Exception as e:
        logger.error(f"Error getting directory list from Hadoop: {e}")

def fetch_file_bytes(file_path):
    try:
        with fs.open(file_path) as f:
            return f.read()
    except Exception as e:
        logger.error(f"Failed to fetch the file {file_path}: {e}")

def fetch_head_from_file(file_path, max_bytes=1024 * 500):  # 500 KB
    try:
        with fs.open(file_path) as f:
            raw_data = f.read(max_bytes)
            decoded_data = raw_data.decode("utf-8", errors="ignore")
            return decoded_data
    except Exception as e:
        logger.error(f"Failed to fetch head of the file {file_path}: {e}")
        return ""