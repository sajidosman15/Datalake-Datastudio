import os
import json
from datetime import datetime
from structlog import get_logger

from app.config import config

logger = get_logger()

def get_hdfs_path(folder_path: str):
    return f"hdfs://{config.hadoop.host}:{config.hadoop.namenode_port}{folder_path}"

def get_list_hdfs_directory(path):
    """
        This function retrieves a list of files and folders along with their metadata from a Hadoop directory.
    """
    try:
        dfs = config.hadoop.get_connection()
        files = dfs.list_status(path)
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
        logger.error(f"Module:HadoopController. Error getting directory list from Hadoop: {e}")

def fetch_file_bytes(file_path):
    """
        This function read the whole file from the Hadoop directory.
    """
    try:
        dfs = config.hadoop.get_connection()
        with dfs.open(file_path) as f:
            return f.read()
    except Exception as e:
        logger.error(f"Module:HadoopController. Failed to fetch the file {file_path}: {e}")

def string_to_json(data: str):
    # If data contains full block of data
    if ']"}' in data:
        valid_data = data.split(']"}', 1)[0]
    else:
        # If data is partial block
        valid_data = data.rsplit("\\n}", 1)[0] + "}"
    valid_data = valid_data + '\\n]'

    clean_data = valid_data.replace('{"value":"', "")
    clean_data = clean_data.replace('\\n', "")
    clean_data = clean_data.replace('\\', "")

    try:
        parsed_json = json.loads(clean_data)
        return json.dumps(parsed_json, indent=2)
    except json.JSONDecodeError as json_err:
        logger.warning(f"Module:HadoopController. Partial JSON decode failed: {json_err}")
        return data

def fetch_head_from_file(file_path, max_bytes=1024 * 500):
    """
    Reads up to 500KB of a file from Hadoop storage and returns a preview.
    - For `.txt` files: returns UTF-8 decoded content.
    - For `.json` files: attempts to return a valid partial JSON array.
    """
    try:
        extension = os.path.splitext(file_path)[-1].lower()
        dfs = config.hadoop.get_connection()
        with dfs.open(file_path) as f:
            raw_data = f.read(max_bytes)

            if extension == ".txt":
                return raw_data.decode("utf-8", errors="ignore")

            elif extension == ".json":
                decoded_data = raw_data.decode("utf-8", errors="ignore")
                return string_to_json(decoded_data)
            
            else:
                return raw_data.decode("utf-8", errors="ignore")

    except Exception as e:
        logger.error(f"Module:HadoopController. Failed to fetch head of the file {file_path}: {e}")
        return ""