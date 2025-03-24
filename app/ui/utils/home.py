import streamlit as st
from structlog import get_logger

from app.ui.utils.connections import connections
from app.ui.utils.data_explorer import data_explorer
from app.ui.utils.connect_source import connect_source

logger = get_logger()

async def home() -> None:
    # Initialize css styles
    with open('app/ui/styles/home.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True) 

    # Initialize menu item if not present in session state
    if "menu_item" not in st.session_state:
        st.session_state.menu_item = "data_explorer"

    # Render left sidebar
    with st.sidebar:
        if st.button("Connections", type="secondary"):
            st.session_state.menu_item = "connections"
        if st.button("Data Explorer", type="secondary"):
            st.session_state.menu_item = "data_explorer"
        if st.button("Delta Storage", type="secondary"):
            st.session_state.menu_item = "delta_storage"
        if st.button("Data Sources", type="secondary"):
            st.session_state.menu_item = "data_sources"

    # Render the corresponding page based on the selected menu item
    menu_functions = {
        "connections": connections,
        "data_explorer": data_explorer,
        "connect_source" : connect_source
    }
    
    if st.session_state.menu_item in menu_functions:
        await menu_functions[st.session_state.menu_item]()
    else:
        st.error(f"Invalid menu item: {st.session_state.menu_item}")
