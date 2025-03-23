import streamlit as st
from structlog import get_logger

from app.ui.helpers.helper import get_gif_image

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
        st.markdown(f"""<p class="table-title">My Data Sources</p>""", unsafe_allow_html=True)

        with st.container():
            st.button("Connect Source", on_click=lambda: setattr(st.session_state, "menu_item", "connect_source"))
                
        with st.container():
            for item in data:
                col1, col2, col3, col4, col5 = st.columns([1, 1.5, 4, 2.2, 1.3])
                
                with col1:
                    st.write(f"**{item['id']}**")
                
                with col2:
                    if item["state"] == "Loading" or item["state"] == "Storing":
                        st.session_state["item"+item['id']] = f"<div class='state_container'> <img class='loading_image' src='{get_gif_image('app/ui/media/loader.gif')}'/> <span class='badge {item['state']}'>{item['state']}</span> </div>"
                    else:
                        st.session_state["item"+item['id']] = f"<span class='badge {item['state']}'>{item['state']}</span>"

                    st.markdown(st.session_state["item"+item['id']], unsafe_allow_html=True)
                
                with col3:
                    st.markdown(f"**{item['name']}**")
                
                with col4:
                    st.markdown(f"<span class='date'>{item['date']}</span>", unsafe_allow_html=True)

                with col5:
                    left, middle, right = st.columns(3)
                    if left.button("",icon=":material/refresh:", key=f"refresh_{item['id']}", help="Refresh", use_container_width=False):
                        left.markdown("You clicked the plain button.")
                    if middle.button("",icon=":material/cloud_download:", key=f"load_{item['id']}", help="Load To Storage", use_container_width=False):
                        middle.markdown("You clicked the emoji button.")
                    if right.button("",icon=":material/delete:", key=f"delete_{item['id']}", help="Delete", use_container_width=False):
                        right.markdown("You clicked the Material button.")