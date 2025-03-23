import streamlit as st
from app.api.nifi import instantiate_flow
from app.database.queries import get_sql_server_table_names

@st.dialog("Select the Tables You Want to Load")
def open_sql_server_popup(source_name, db_url, db_name, db_username, db_password):
    tables = get_sql_server_table_names(db_url, db_name, db_username, db_password)
    if tables:
        selected_tables = {}
        for table in tables:
            selected_tables[table] = st.checkbox(table)

        if st.button("Submit"):
            selected = [table for table, checked in selected_tables.items() if checked]
            
            if selected:
                st.success(f"Selected tables: {', '.join(selected)}")
                instantiate_flow(db_url,db_name,db_username,db_password,selected)
            else:
                st.toast("⛔ No tables selected.")
    else:
        st.write("Failed to Connect the Source")

async def connect_source() -> None:
    with open('app/ui/styles/connect_source.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True) 

    st.title("Connect New Data Source")

    source_name = st.text_input("Data Source Name *", help="Give a name of your data source.")

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
            open_sql_server_popup(source_name, db_url, db_name, db_username, db_password)
        else:
            st.toast("⛔ Please fill all required fields.")
