import streamlit as st
from app.api.nifi import instantiate_flow


# Streamlit UI
st.title("NiFi Flow Deployer")

db_url = st.text_input("Database URL")
db_name = st.text_input("Database Name")
db_username = st.text_input("Database Username")
db_password = st.text_input("Database Password", type="password")
deploy_button = st.button("Deploy Flow")


# Trigger flow deployment
if deploy_button:
    if db_url and db_name and db_username and db_password:
        instantiate_flow(db_url,db_name,db_username,db_password)
    else:
        st.warning("Please provide all required database credentials")
