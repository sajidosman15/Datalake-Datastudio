import streamlit as st
import webbrowser
from structlog import get_logger

from app.config import get_api_server

from app.models.dataset import Dataset

logger = get_logger()

async def data_sources() -> None:
    with open('app/views/styles/data_sources.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True) 

    st.title("Data Sources")

    datasets = Dataset.list_all()

    with st.container():
        st.markdown(f"""<p class="table-title">My Data Sources</p>""", unsafe_allow_html=True)

        with st.container(key="tools_container"):
            st.button("Data Models", key="data_models_button")
            st.button("SQL Query Tool", key="sql_query_tool_button")
                
        with st.container():
            for record in datasets:
                col1, col2, col3, col4 = st.columns([3, 4, 3, 2])
                
                with col1:
                    st.markdown(f"{record.dataset_name}")

                with col2:
                    link = f"{get_api_server()}/api/{record.api_version}/data/{record.dataset_owner}/{record.table_name}"
                    st.text_input("Source URL", value=link, key=f"source_url_{record.id}", disabled=True, label_visibility="hidden")
                
                with col3:
                    st.markdown(f"<span class='date'>{record.create_date}</span>", unsafe_allow_html=True)

                with col4:
                    btn1, btn2, btn3, btn4 = st.columns(4)
                    if btn1.button("",icon=":material/analytics:", key=f"analytics_{record.id}", help="Analytics", use_container_width=False):
                        btn1.markdown("You clicked the plain button.")
                    if btn2.button("",icon=":material/network_intel_node:", key=f"network_intel_node_{record.id}", help="Machine Learning", use_container_width=False):
                        btn2.markdown("You clicked the emoji button.")
                    if btn3.button("",icon=":material/edit_document:", key=f"edit_document_{record.id}", help="Update BI Link", use_container_width=False):
                        btn3.markdown("You clicked the emoji button.")
                    if btn4.button("",icon=":material/bar_chart_4_bars:", key=f"bar_chart_4_bars_{record.id}", help="Power BI", disabled= False if record.dashboard_url != "" else True, use_container_width=False):
                        webbrowser.open(record.dashboard_url)