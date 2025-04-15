import streamlit as st
from structlog import get_logger

from app.views.helpers.helper import get_gif_image

from app.models.connection import Connection

logger = get_logger()

@st.dialog("Are You Sure?")
def delete_popup(record : Connection):
    st.write("You won't be able to revert this!")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("❌ No, Cancel!", key=f"cancel_delete_{record.id}"):
            st.rerun()

    with col2:
        if st.button("✅ Yes, Delete!", key=f"confirm_delete_{record.id}"):
            delete = record.delete()
            if delete:
                st.session_state.popupmsg = "✅ The Data Source Deleted Successfully."
            else:
                st.session_state.popupmsg = "❌ Failed to Delete the Data Source."

            st.session_state.popup = True
            st.rerun()

async def connections() -> None:
    with open('app/views/styles/connections.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True) 

    st.title("Connections List")

    connections = Connection.list_all()

    with st.container():
        st.markdown(f"""<p class="table-title">My Data Connections</p>""", unsafe_allow_html=True)

        with st.container():
            st.button("Connect Source", on_click=lambda: setattr(st.session_state, "menu_item", "connect_source"))
                
        with st.container():
            for record in connections:
                col1, col2, col3, col4, col5 = st.columns([1, 1.5, 4, 2.2, 1.3])
                
                with col1:
                    st.write(f"**#{record.id}**")
                
                with col2:
                    if record.state == "Loading" or record.state == "Storing":
                        st.session_state["item"+record.id] = f"<div class='state_container'> <img class='loading_image' src='{get_gif_image('app/views/media/loader.gif')}'/> <span class='badge {record.state}'>{record.state}</span> </div>"
                    else:
                        st.session_state["item"+record.id] = f"<span class='badge {record.state}'>{record.state}</span>"

                    st.markdown(st.session_state["item"+record.id], unsafe_allow_html=True)
                
                with col3:
                    st.markdown(f"**{record.connection_name}**")
                
                with col4:
                    st.markdown(f"<span class='date'>{record.create_date}</span>", unsafe_allow_html=True)

                with col5:
                    left, middle, right = st.columns(3)
                    if left.button("",icon=":material/refresh:", key=f"refresh_{record.id}", help="Refresh", use_container_width=False):
                        left.markdown("You clicked the plain button.")
                    if middle.button("",icon=":material/cloud_download:", key=f"load_{record.id}", help="Load To Storage", use_container_width=False):
                        middle.markdown("You clicked the emoji button.")
                    if right.button("",icon=":material/delete:", key=f"delete_{record.id}", help="Delete", use_container_width=False):
                        delete_popup(record)