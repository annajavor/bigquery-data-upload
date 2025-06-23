import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import re

# --- CONFIG ---
st.set_page_config(page_title="The Port", page_icon=":anchor:", layout="wide")
st.title(":anchor: The Port")
st.header(":arrow_down: Download CSV Export from BigQuery")
st.info(
    """Use this tool to download data from BigQuery by selecting one of the available tables. 
    Choose a dataset, define the date range, and select the client or clients you'd like to retrieve data for.
    """
)

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

def login():
    st.title("Login")
    username_input = st.text_input("Username")
    password_input = st.text_input("Password", type="password")
    
    if st.button("Login"):
        if (
            username_input == st.secrets["username"] and 
            password_input == st.secrets["app_password"]
        ):
            st.session_state.logged_in = True
            st.rerun()  # refresh page after login
        else:
            st.error("Incorrect username or password")

if not st.session_state.logged_in:
    login()
    st.stop()

# --- CONSTANTS ---
PROJECT_ID = "trimark-tdp"

def init_bigquery_client():
    """Initialize BigQuery client with service account credentials"""
    try:
        if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
        else:
            credentials = service_account.Credentials.from_service_account_file(
                '/Users/trimark/Desktop/Jupyter_Notebooks/trimark-tdp-87c89fbd0816.json'
            )
        
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        return client
    except Exception as e:
        st.error(f"Error initializing BigQuery client: {str(e)}")
        return None

# --- TABLE OPTIONS ---
tables = {
    "All Paid Media": "trimark-tdp.master.all_paidmedia",
    "All GA4": "trimark-tdp.master.all_ga4",
    "All Leads": "trimark-tdp.master.all_leads",
    "All Form Leads": "trimark-tdp.master.all_form_table",
    "All GMB": "trimark-tdp.master.all_gmb"
}

# Table selectbox
selected_table = st.selectbox(
    "Select a BigQuery Table", 
    list(tables.keys()), 
    key="table_select"
)
table_path = tables[selected_table]

# Dynamically set client column based on selected table
if selected_table in ["All Leads", "All Form Leads"]:
    client_col = "Client_Name"
else:
    client_col = "client_name"

# Date inputs
start_date = st.date_input("Start Date", datetime(2024, 1, 1), key="start_date")
end_date = st.date_input("End Date", datetime.today(), key="end_date")

# Initialize BigQuery client
client = init_bigquery_client()
if client is None:
    st.stop()

# Fetch unique clients
client_query = f"SELECT DISTINCT {client_col} FROM `{table_path}` WHERE {client_col} IS NOT NULL"
try:
    clients_df = client.query(client_query).to_dataframe()
    clients = sorted(clients_df[client_col].dropna().unique())
    selected_clients = st.multiselect(
        "Select one or more Clients", 
        clients, 
        key="client_multiselect"
    )
except Exception as e:
    st.error(f"Could not fetch clients: {e}")
    selected_clients = []

# --- QUERY & DOWNLOAD ---
if st.button("Run Query and Download"):
    if selected_clients:
        query = f"""
            SELECT *
            FROM `{table_path}`
            WHERE {client_col} IN UNNEST(@clients)
              AND DATE(date) BETWEEN @start_date AND @end_date
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("clients", "STRING", selected_clients),
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        )
        try:
            df = client.query(query, job_config=job_config).to_dataframe()
            st.dataframe(df)
            csv = df.to_csv(index=False)
            st.download_button("Download CSV", data=csv, file_name="bigquery_data.csv", mime="text/csv")
        except Exception as e:
            st.error(f"❌ Query failed: {e}")
    else:
        st.warning("Please select at least one client.")

# --- UPLOAD TO BIGQUERY ---
import re
from google.cloud import bigquery

# --- Upload Section ---
st.header("⬆️ Upload CSV to BigQuery")
st.info(
    """Use this tool to upload CSV data to BigQuery. 
    Click 'Browse files' to select your file, then choose whether to create a new table, append to an existing one, or replace an existing table. 
    If creating a new table, enter a name using lowercase letters and underscores (e.g., your_table_name)."""
)

uploaded_file = st.file_uploader("Upload CSV", type="csv", key="file_uploader")

if uploaded_file is not None:
    df_upload = pd.read_csv(uploaded_file)

    def clean_column_name(column_name):
        column_name = column_name.replace(' ', '_')
        column_name = re.sub(r'[^\w\s]', '', column_name)
        return column_name

    df_upload.columns = [clean_column_name(col) for col in df_upload.columns]
    st.dataframe(df_upload)

    st.subheader("Upload Settings")

    dataset_id = "sftp_uploads"
    
    write_disposition = st.selectbox(
        "Choose upload mode",
        options=["Create new table", "Append to existing table", "Replace existing table"]
    )

    # Fetch existing tables
    existing_tables = []
    try:
        dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
        tables_list = client.list_tables(dataset_ref)
        existing_tables = [table.table_id for table in tables_list]
    except Exception as e:
        st.warning(f"⚠️ Could not fetch existing tables: {e}")

    # Logic based on selected upload mode
    if write_disposition == "Create new table":
        table_id = st.text_input("New Table Name (lowercase_with_underscores)", "your_table_name", key="new_table")
    else:
        table_id = st.selectbox("Select Existing Table", existing_tables, key="existing_table")

    # Map write disposition for BigQuery
    disposition_map = {
        "Create new table": "WRITE_EMPTY",
        "Append to existing table": "WRITE_APPEND",
        "Replace existing table": "WRITE_TRUNCATE"
    }

    if st.button("Upload to BigQuery", key="upload_button"):
        table_ref = f"{client.project}.{dataset_id}.{table_id}"
        job_config = bigquery.LoadJobConfig(write_disposition=disposition_map[write_disposition])

        try:
            job = client.load_table_from_dataframe(df_upload, table_ref, job_config=job_config)
            job.result()
            st.success(f"✅ Uploaded to `{table_ref}` using mode: {write_disposition}")
        except Exception as e:
            st.error(f"❌ Upload failed: {e}")


