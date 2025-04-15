import streamlit as st
import webbrowser
from structlog import get_logger

from app.models.connection import Connection

logger = get_logger()

async def data_sources() -> None:
    with open('app/views/styles/data_sources.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True) 

    st.title("Data Sources")

    connections = Connection.list_all()

    with st.container():
        st.markdown(f"""<p class="table-title">My Data Sources</p>""", unsafe_allow_html=True)

        with st.container(key="tools_container"):
            st.button("Data Models", key="data_models_button")
            st.button("SQL Query Tool", key="sql_query_tool_button")
                
        with st.container():
            for record in connections:
                col1, col2, col3, col4 = st.columns([3, 4, 3, 2])
                
                with col1:
                    st.markdown(f"{record.connection_name}")

                with col2:
                    link = "https://chatgpt.com/c/67fe2de9-c4a0-8000-8a00-57029dee729f"
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
                    if btn4.button("",icon=":material/bar_chart_4_bars:", key=f"bar_chart_4_bars_{record.id}", help="Power BI", use_container_width=False):
                        target_url = "https://app.powerbi.com/view?r=eyJrIjoiNjdiZjJkZDktOGQ2ZS00NjcwLTg1MmUtYTk0YzIwYzRiZGI4IiwidCI6IjM3ZWYzZDE5LTE2NTEtNDQ1Mi1iNzYxLWRjMjQxNGJmMDQxNiIsImMiOjh9"
                        webbrowser.open(target_url)