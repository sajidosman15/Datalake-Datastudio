import streamlit as st
from structlog import get_logger

logger = get_logger()

async def data_explorer() -> None:
    st.title("Data Explorer")
