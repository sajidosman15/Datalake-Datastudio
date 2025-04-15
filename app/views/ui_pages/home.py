import streamlit as st
from structlog import get_logger

from app.views.ui_pages.connections import connections
from app.views.ui_pages.data_explorer import data_explorer
from app.views.ui_pages.delta_storage import delta_storage
from app.views.ui_pages.connect_source import connect_source
from app.views.ui_pages.data_sources import data_sources

logger = get_logger()

async def home() -> None:
    # Initialize css styles
    with open('app/views/styles/home.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True) 

    # Initialize the popup message
    if "popup" not in st.session_state:
        st.session_state.popup = False

    if st.session_state.popup == True:
        st.toast(st.session_state.popupmsg)
        st.session_state.popup = False

    # Initialize menu item if not present in session state
    if "menu_item" not in st.session_state:
        st.session_state.menu_item = "connections"

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
        if st.button("Machine Learning", type="secondary"):
            st.session_state.menu_item = "machine_learning"

    # Render the corresponding page based on the selected menu item
    menu_functions = {
        "connections": connections,
        "data_explorer": data_explorer,
        "delta_storage": delta_storage,
        "connect_source" : connect_source,
        "data_sources" : data_sources
    }
    
    if st.session_state.menu_item in menu_functions:
        await menu_functions[st.session_state.menu_item]()
    else:
        st.error(f"Invalid menu item: {st.session_state.menu_item}")
