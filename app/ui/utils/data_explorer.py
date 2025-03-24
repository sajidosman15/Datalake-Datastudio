import streamlit as st
from structlog import get_logger
import pyhdfs

logger = get_logger()

 # Hadoop Configuration
HDFS_HOST = "10.10.12.48"
HDFS_PORT = "9870"
fs = pyhdfs.HdfsClient(hosts=f"{HDFS_HOST}:{HDFS_PORT}",user_name="scibddlp")

# Function to list files and folders
def list_hdfs_directory(path):
    try:
        files = fs.list_status(path)
        return sorted(
            [{"name": f["pathSuffix"], "type": "dir" if f["type"] == "DIRECTORY" else "file"} for f in files],
            key=lambda x: (x["type"] != "dir", x["name"])  # Sort: folders first, then files
        )
    except Exception as e:
        return str(e)

# Function to read file contents
def read_hdfs_file(file_path):
    try:
        with fs.open(file_path) as f:  # Only pass the file path as a positional argument
            return f.read().decode("utf-8")  # Decode the file content (assumes it's text)
    except Exception as e:
        return str(e)

    
async def data_explorer() -> None:
    # Initialize session state for current path
    if "current_path" not in st.session_state:
        st.session_state.current_path = "/DataLake"

    st.title("Data Explorer")

    st.write(f"📂 **Current Directory:** `{st.session_state.current_path}`")

    # Navigate up one level
    if st.session_state.current_path != "/DataLake":
        if st.button("⬆ Go Up"):
            st.session_state.current_path = "/".join(st.session_state.current_path.rstrip("/").split("/")[:-1]) or "/"
            st.rerun()

    # List files and folders
    items = list_hdfs_directory(st.session_state.current_path)

    if isinstance(items, str):
        st.error(items)
    else:
        for item in items:
            item_path = f"{st.session_state.current_path.rstrip('/')}/{item['name']}"
            
            if item["type"] == "dir":
                if st.button(f"📁 {item['name']}"):
                    st.session_state.current_path = item_path
                    st.rerun()
            else:
                if item['name'] != "_SUCCESS":
                    if st.button(f"📄 {item['name']}"):
                        file_content = read_hdfs_file(item_path)
                        st.text_area(f"📖 {item['name']}", file_content, height=300)


