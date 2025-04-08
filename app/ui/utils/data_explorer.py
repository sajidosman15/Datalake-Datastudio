import io
import streamlit as st
from app.api.hadoop import get_list_hdfs_directory, fetch_file_bytes, fetch_head_from_file

def update_current_path(item_type, item_path):
    st.session_state.file_triggered = False
    if item_type == "Folder":
        st.session_state.current_path = item_path
        st.session_state.display_path = st.session_state.current_path[:1] + st.session_state.current_path[10:]
        st.session_state.display_path = st.session_state.display_path.title()
        st.session_state.display_path = '... ' + st.session_state.display_path[len(st.session_state.display_path) - 90:] if len(st.session_state.display_path) > 90 else st.session_state.display_path
    else:
        file_bytes = fetch_file_bytes(item_path)
        if isinstance(file_bytes, bytes):
            st.session_state.file_data = file_bytes
            st.session_state.sample_data = fetch_head_from_file(item_path)
            st.session_state.file_name = item_path.split("/")[-1]
            st.session_state.file_triggered = True
    st.rerun()

async def data_explorer() -> None:
    with open('app/ui/styles/data_explorer.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True) 

    if "current_path" not in st.session_state:
        st.session_state.current_path = "/DataLake"
    if "display_path" not in st.session_state:
        st.session_state.display_path = "/"
    if "file_triggered" not in st.session_state:
        st.session_state.file_triggered = False

    st.title("Data Explorer")

    with st.container():
        if st.button("",icon=":material/arrow_back:", help="Go Back", use_container_width=False, disabled= False if st.session_state.current_path != "/DataLake" else True):
            item_path = "/".join(st.session_state.current_path.rstrip("/").split("/")[:-1]) or "/"
            update_current_path("Folder", item_path)
        st.write(f"{st.session_state.display_path}")

    # Get list of files and folders from Hadoop
    items = get_list_hdfs_directory(st.session_state.current_path)

    with st.container():
        if isinstance(items, str):
            st.error(items)
        else:
            col1, col2, col3, col4 = st.columns([4, 3, 2, 2])
            with col1:
                st.write("Name")
            with col2:
                st.write("Date Modified")
            with col3:
                st.write("Type")
            with col4:
                st.write("Size")

            for item in items:
                item_path = f"{st.session_state.current_path.rstrip('/')}/{item['name']}"
                
                col1, col2, col3, col4 = st.columns([4, 3, 2, 2])
                if item["type"] == "Folder":
                    item_icon = "📁"
                    item_size = "--"
                else:
                    item_icon = "📄"
                    item_size = str(item['size_kb']) + " KB"

                if item["name"] != "_SUCCESS":
                    with col1:
                        item_name = item['name'][:30] + ' ...' if len(item['name']) > 30 else item['name']
                        item_name = item_name.title() # Capitalize each word
                        if st.button(f"{item_icon} {item_name}", key=item_path+"1", help=item['name']):
                            update_current_path(item['type'], item_path)

                    with col2:
                        if st.button(f"{item['mod_time']}", key=item_path+"2"):
                            update_current_path(item['type'], item_path)

                    with col3:
                        if st.button(f"{item['type']}", key=item_path+"3"):
                            update_current_path(item['type'], item_path)

                    with col4:
                        if st.button(f"{item_size}", key=item_path+"4"):
                            update_current_path(item['type'], item_path)

                    if st.session_state.file_triggered:
                        with st.container():
                            col1, col2 = st.columns([8,2])
                            with col1:
                                st.write(f"📖 {st.session_state.file_name}")
                            with col2:
                                st.download_button(
                                    icon=":material/download:",
                                    label="Download",
                                    data=io.BytesIO(st.session_state.file_data),
                                    file_name=st.session_state.file_name,
                                    mime="application/octet-stream",
                                    key="download_button"
                                )
                        st.text_area("Text Area", st.session_state.sample_data, label_visibility="hidden", height=300, disabled=True)

