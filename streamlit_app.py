import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

# --- CONFIG ---
st.set_page_config(page_title="The Port", page_icon=":ocean:", layout="wide")
st.title(":ocean: The Port")
st.info(
    "Use this tool to download data from BigQuery for analysis or reference, or to upload data for storage and processing."
)

# --- CONSTANTS ---
PROJECT_ID = "trimark-tdp"

# --- INIT BIGQUERY CLIENT ---
def init_bigquery_client():
    try:
        if "gcp_service_account" in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
        else:
            credentials = service_account.Credentials.from_service_account_file(
                '/Users/annajavor/Desktop/python_notebooks/research/skin_the_cat/trimark-tdp-87c89fbd0816.json'
            )
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        return client
    except Exception as e:
        st.error(f"Error initializing BigQuery client: {str(e)}")
        return None

client = init_bigquery_client()

if client is None:
    st.stop()

# --- TABLE OPTIONS ---
tables = {
    "All Paid Media": "trimark-tdp.master.allpaidmedia",
    "Meta Ads": "trimark-tdp.meta.ads_data",
    "Google Ads": "trimark-tdp.google.ads_data"
}

selected_table = st.selectbox("Select a BigQuery Table", list(tables.keys()))
table_path = tables[selected_table]

# --- DATE FILTERING ---
start_date = st.date_input("Start Date", datetime(2024, 1, 1))
end_date = st.date_input("End Date", datetime.today())

# --- CLIENT FILTERING ---
client_query = f"SELECT DISTINCT client FROM `{table_path}` WHERE client IS NOT NULL"
try:
    clients_df = client.query(client_query).to_dataframe()
    clients = sorted(clients_df["client"].dropna().unique())
    selected_client = st.selectbox("Select a Client", clients)
except Exception as e:
    st.error(f"Could not fetch clients: {e}")
    clients = []
    selected_client = None

# --- QUERY & DOWNLOAD ---
if st.button("Run Query and Download"):
    if selected_client:
        query = f"""
            SELECT *
            FROM `{table_path}`
            WHERE client = @client
              AND DATE(date) BETWEEN @start_date AND @end_date
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("client", "STRING", selected_client),
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
        st.warning("Please select a valid client.")

# --- UPLOAD TO BIGQUERY ---
st.header("⬆️ Upload CSV to BigQuery")
uploaded_file = st.file_uploader("Upload CSV", type="csv")

if uploaded_file is not None:
    df_upload = pd.read_csv(uploaded_file)
    st.dataframe(df_upload)

    dataset_id = st.text_input("Dataset ID", "master")
    table_id = st.text_input("Table Name", "your_table_name")

    if st.button("Upload to BigQuery"):
        table_ref = f"{client.project}.{dataset_id}.{table_id}"
        try:
            job = client.load_table_from_dataframe(df_upload, table_ref)
            job.result()
            st.success(f"✅ Uploaded to {table_ref}")
        except Exception as e:
            st.error(f"❌ Upload failed: {e}")
