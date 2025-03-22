import streamlit as st
from structlog import get_logger

logger = get_logger()

async def connections() -> None:
    with open('app/ui/styles/connections.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True) 

    st.title("Connections List")

    data = [
        {"id": "#8793", "state": "Loading", "name": "Suchana Nutrition Data", "date": "3 Jan, 4:35 PM"},
        {"id": "#3452", "state": "Loaded", "name": "PMT Client Data", "date": "4 Feb, 2:35 PM"},
        {"id": "#6453", "state": "Storing", "name": "Prime Health Indicators", "date": "17 Jan, 4:56 PM"},
        {"id": "#7456", "state": "Stored", "name": "SQL Server Management Data", "date": "29 Dec, 4:35 PM"},
    ]

    with st.container():
        st.markdown(f"""
            <p class="table-title">
                My Data Sources
            </p>
        """, unsafe_allow_html=True)

        with st.container():
            st.button("Connect Source")

        with st.container():
            for item in data:
                col1, col2, col3, col4, col5 = st.columns([1, 1.5, 4.5, 2, 1])
                
                with col1:
                    st.write(f"**{item['id']}**")
                
                with col2:
                    st.markdown(f"<span class='badge {item['state']}'>{item['state']}</span>", unsafe_allow_html=True)
                
                with col3:
                    st.markdown(f"**{item['name']}**")
                
                with col4:
                    st.markdown(f"<span class='date'>{item['date']}</span>", unsafe_allow_html=True)

                with col5:
                    st.write("Action")