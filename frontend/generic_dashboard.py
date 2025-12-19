import json
import requests
import streamlit as st
import pandas as pd

API_URL = "http://dev1.bilinearlabs.io:9720/raw_query"

st.set_page_config(page_title="Generic Dashboard", layout="wide")
st.title("Generic Dashboard")

@st.cache_data(show_spinner=False, ttl=600)
def get_descriptor() -> pd.DataFrame:
    sql = "SELECT * FROM event_descriptor"
    resp = requests.post(
        API_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({"query": sql}),
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json().get("query_result", [])
    return pd.DataFrame(data)

def build_table_name(row) -> str:
    name = row["event_name"].lower().replace(" ", "_")
    return f"event_{name}_{row['event_hash']}"

descriptor_df = get_descriptor()

if descriptor_df.empty:
    st.error("Could not fetch event_descriptor table.")
    st.stop()

# Build mapping of label -> table name
_descriptor = descriptor_df.copy()
_descriptor["table_name"] = _descriptor.apply(build_table_name, axis=1)

options = _descriptor["event_name"].tolist()
selected_event = st.selectbox("Select event", options)
selected_row = _descriptor[_descriptor["event_name"] == selected_event].iloc[0]
selected_table = selected_row["table_name"]
signature = selected_row.get("event_signature", "")

st.caption(f"Querying latest 100 rows from `{selected_table}`\n\nSignature: `{signature}`")

sql_latest = f"SELECT * FROM {selected_table} ORDER BY block_number DESC LIMIT 100"

# Interactive SQL editor ---
sql_input = st.text_area(
    "Edit SQL query and press Run to refresh the table", value=sql_latest, height=150, key="sql_editor"
)
run_clicked = st.button("Run Query")

# Determine which query to run
query_to_run = sql_input if run_clicked else sql_latest
# Create the container after the text area so the table appears below it
container = st.container()

@st.cache_data(show_spinner=False, ttl=300)
def run_query(q: str) -> pd.DataFrame:
    resp = requests.post(
        API_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({"query": q}),
        timeout=20,
    )
    resp.raise_for_status()
    return pd.DataFrame(resp.json().get("query_result", []))

try:
    df_result = run_query(query_to_run)
    container.dataframe(df_result, use_container_width=True)
except Exception as e:
    st.error(f"Query failed: {e}")
