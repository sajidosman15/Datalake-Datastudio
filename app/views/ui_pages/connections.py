import streamlit as st
from structlog import get_logger

from app.views.helpers.helper import get_gif_image

from app.models.connection import Connection

logger = get_logger()

async def connections() -> None:
    if "popup" not in st.session_state:
        st.session_state.popup = False

    if st.session_state.popup == True:
        st.toast(st.session_state.popupmsg)
        st.session_state.popup = False

    with open('app/views/styles/connections.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True) 

    st.title("Connections List")

    data = Connection.list_all()

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
                        st.session_state["item"+item['id']] = f"<div class='state_container'> <img class='loading_image' src='{get_gif_image('app/views/media/loader.gif')}'/> <span class='badge {item['state']}'>{item['state']}</span> </div>"
                    else:
                        st.session_state["item"+item['id']] = f"<span class='badge {item['state']}'>{item['state']}</span>"

                    st.markdown(st.session_state["item"+item['id']], unsafe_allow_html=True)
                
                with col3:
                    st.markdown(f"**{item['connection_name']}**")
                
                with col4:
                    st.markdown(f"<span class='date'>{item['create_date']}</span>", unsafe_allow_html=True)

                with col5:
                    left, middle, right = st.columns(3)
                    if left.button("",icon=":material/refresh:", key=f"refresh_{item['id']}", help="Refresh", use_container_width=False):
                        left.markdown("You clicked the plain button.")
                    if middle.button("",icon=":material/cloud_download:", key=f"load_{item['id']}", help="Load To Storage", use_container_width=False):
                        middle.markdown("You clicked the emoji button.")
                    if right.button("",icon=":material/delete:", key=f"delete_{item['id']}", help="Delete", use_container_width=False):
                        right.markdown("You clicked the Material button.")