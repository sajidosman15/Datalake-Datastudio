import asyncio
import streamlit as st

from app.ui.utils.home import home as home

async def main():
    st.set_page_config(page_title="TechHub Data Studio",layout="wide")
    st.sidebar.image("app/ui/media/techhub-logo-red.png", use_container_width=True)

    await home()


asyncio.run(main())