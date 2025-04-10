import streamlit as st
from app.controllers.nifi import instantiate_flow

from app.models.connection import Connection
from app.models.connector.sql_server import SQLServer

@st.dialog("Select the Tables You Want to Load")
def open_sql_server_popup(connection:Connection, sql_server:SQLServer):
    tables = sql_server.get_table_names()
    if tables:
        selected_tables = {}
        for table in tables:
            selected_tables[table] = st.checkbox(table)

        if st.button("Submit"):
            tables = [table for table, checked in selected_tables.items() if checked]
            
            if tables:
                sql_server.tables = tables
                connection.connection_properties = sql_server.to_connection_properties()
                start_flow = instantiate_flow(connection)
                if start_flow:
                    st.session_state.popupmsg = "✅ The Data Source Connected Successfully."
                else:
                    st.session_state.popupmsg = "❌ Failed To Connect The Source. Try Again Later."
                st.session_state.popup = True
                st.session_state.menu_item = "connections"
                st.rerun()
            else:
                st.error("⛔ Please select tables from the list.")
    else:
        # Task: Check Error or Empty Database
        st.write("Failed to Connect the Source")

async def connect_source() -> None:
    with open('app/views/styles/connect_source.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True) 

    st.title("Connect New Data Source")

    source_name = st.text_input("Data Source Name *", help="Give a name of your data source.", max_chars=40)

    source_lists = ["Microsoft SQL Server"]
    source_type = st.selectbox(label="Data Source Type *", options=source_lists, help="Select the data source.")
    
    left_column, right_column = st.columns(2)
    db_url = left_column.text_input("Database URL *", help="Enter the data source url.")
    db_name = right_column.text_input("Database Name *", help="Enter the database name you want's to connect.")
    db_username = left_column.text_input("Database Username *", help="Enter database username.")
    db_password = right_column.text_input("Database Password *", type="password", help="Enter database password.")

    deploy_button = st.button("Connect",help="Connect the data source.")

    # Trigger flow deployment
    if deploy_button:
        if source_type == "Microsoft SQL Server" and source_name and db_url and db_name and db_username and db_password:
            connection = Connection(connection_name=source_name, source_type=source_type)
            sql_server = SQLServer(db_url=db_url, db_name=db_name, db_username=db_username, db_password=db_password)
            open_sql_server_popup(connection, sql_server)
        else:
            st.toast("⛔ Please fill all required fields.")
