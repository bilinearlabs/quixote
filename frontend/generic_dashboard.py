import json
import requests
import streamlit as st
import pandas as pd
from pathlib import Path

API_BASE = "http://127.0.0.1:9720"
API_URL = f"{API_BASE}/raw_query"
SCHEMA_URL = f"{API_BASE}/db_schema"
EVENTS_URL = f"{API_BASE}/list_events"
CONTRACTS_URL = f"{API_BASE}/list_contracts"

# Get the icon path (relative to this script)
SCRIPT_DIR = Path(__file__).parent
ICON_PATH = SCRIPT_DIR.parent / "assets" / "quixote-icon.png"

st.set_page_config(
    page_title="Quixote Dashboard",
    page_icon=str(ICON_PATH) if ICON_PATH.exists() else "🗡️",
    layout="wide",
    initial_sidebar_state="expanded"
)


def safe_int(value, default=0) -> int:
    """Safely convert a value to int, handling None, strings, etc."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


@st.cache_data(show_spinner=False, ttl=30)
def get_contracts() -> list[dict]:
    """Fetch indexed contracts."""
    try:
        resp = requests.get(CONTRACTS_URL, timeout=20)
        resp.raise_for_status()
        return resp.json().get("contracts", [])
    except Exception:
        return []


@st.cache_data(show_spinner=False, ttl=5)
def get_events_status() -> list[dict]:
    """Fetch event indexing status from /list_events endpoint."""
    try:
        resp = requests.get(EVENTS_URL, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        return data.get("events", [])
    except Exception as e:
        st.sidebar.error(f"API error: {e}")
        return []


@st.cache_data(show_spinner=False, ttl=60)
def get_schema() -> list[dict]:
    """Fetch database schema."""
    try:
        resp = requests.get(SCHEMA_URL, timeout=20)
        resp.raise_for_status()
        return resp.json().get("schema", [])
    except Exception:
        return []


@st.cache_data(show_spinner=False, ttl=60)
def get_event_tables() -> list[str]:
    """Extract event table names from schema."""
    schema = get_schema()
    event_tables = []
    for table_obj in schema:
        for table_name in table_obj.keys():
            if table_name.startswith("event_") and table_name != "event_descriptor":
                event_tables.append(table_name)
    return sorted(event_tables)


# --- Sidebar: Events Status ---
with st.sidebar:
    # Logo and title
    col1, col2 = st.columns([1, 3])
    with col1:
        if ICON_PATH.exists():
            st.image(str(ICON_PATH), width=50)
    with col2:
        st.markdown("### Quixote")

    st.divider()
    st.markdown("#### 📊 Event Status")

    # Events status section - fetch from /list_events
    events_status = get_events_status()

    if events_status:
        for event in events_status:
            # Get event name (API uses camelCase)
            name = event.get("eventName") or "Unknown"

            # Get block numbers (API uses camelCase: firstBlock, lastBlock, eventCount)
            first_block = safe_int(event.get("firstBlock"))
            last_block = safe_int(event.get("lastBlock"))
            event_count = safe_int(event.get("eventCount"))

            # Build status message
            if last_block > 0 and event_count > 0:
                status_msg = f"**{name}**\n\n📦 {first_block:,} → {last_block:,}\n\n📊 {event_count:,} events"
                st.success(status_msg)
            elif last_block > 0:
                status_msg = f"**{name}**\n\n📦 {first_block:,} → {last_block:,}"
                st.info(status_msg)
            elif first_block > 0:
                status_msg = f"**{name}**\n\n📦 Starting from {first_block:,}"
                st.info(status_msg)
            else:
                st.warning(f"**{name}**\n\nNo block data yet")
    else:
        st.caption("No events indexed yet")

    st.divider()
    if st.button("🔄 Refresh", key="refresh_btn"):
        st.cache_data.clear()
        st.rerun()


# --- Main Content ---

# Header with logo - using HTML for proper vertical alignment
if ICON_PATH.exists():
    st.markdown(
        f"""
        <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 8px;">
            <img src="data:image/png;base64,{__import__('base64').b64encode(open(str(ICON_PATH), 'rb').read()).decode()}" width="60" height="60" style="vertical-align: middle;">
            <h1 style="margin: 0; padding: 0;">Quixote Dashboard</h1>
        </div>
        """,
        unsafe_allow_html=True
    )
else:
    st.title("Quixote Dashboard")

# Subtitle with spacing
st.markdown("")
st.markdown("**Blazing-fast blockchain event indexing, powered by Rust and DuckDB** 🦀")
st.markdown("")

# Indexed Contracts Section
contracts = get_contracts()
if contracts:
    with st.expander("📜 Indexed Contracts", expanded=True):
        for contract in contracts:
            address = contract.get("contract_address", "Unknown")
            name = contract.get("contract_name")
            if name:
                st.code(f"{name}: {address}", language=None)
            else:
                st.code(address, language=None)

# Database Schema Section
with st.expander("📋 Database Schema", expanded=False):
    schema = get_schema()
    if schema:
        schema_data = []
        for table_obj in schema:
            for table_name, columns in table_obj.items():
                for col_name, col_type in columns.items():
                    schema_data.append({
                        "Table": table_name,
                        "Column": col_name,
                        "Type": col_type
                    })

        if schema_data:
            schema_df = pd.DataFrame(schema_data)
            tables = schema_df["Table"].unique()

            # Display tables in a grid
            cols = st.columns(3)
            for i, table in enumerate(tables):
                with cols[i % 3]:
                    st.markdown(f"**`{table}`**")
                    table_cols = schema_df[schema_df["Table"] == table][["Column", "Type"]]
                    st.dataframe(table_cols, hide_index=True, height=200)
    else:
        st.info("No tables found in database")

st.divider()

# Query Section
st.markdown("### 🔍 Query Events")

event_tables = get_event_tables()

if not event_tables:
    st.warning("No event tables found. Start indexing events to see them here.")
    st.stop()

# Table selector (no label, just the dropdown)
selected_table = st.selectbox("Table", event_tables, label_visibility="collapsed")

sql_latest = f"SELECT * FROM {selected_table} ORDER BY block_number DESC LIMIT 100"

# SQL editor
sql_input = st.text_area(
    "SQL Query",
    value=sql_latest,
    height=100,
    key="sql_editor",
    label_visibility="collapsed"
)

col1, col2, col3 = st.columns([1, 1, 6])
with col1:
    run_clicked = st.button("▶️ Run", type="primary", key="run_btn")
with col2:
    if st.button("🔄 Clear Cache", key="clear_btn"):
        st.cache_data.clear()
        st.rerun()

query_to_run = sql_input if run_clicked else sql_latest


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
    with st.spinner("Executing query..."):
        df_result = run_query(query_to_run)

    # Results header
    st.markdown(f"**Results** · {len(df_result):,} rows")
    st.dataframe(df_result, height=500)

except Exception as e:
    st.error(f"Query failed: {e}")

# Footer
st.divider()
st.markdown(
    """
    <div style='text-align: center; font-size: 0.85rem;'>
        <strong>Quixote Indexer</strong> · Built with ❤️ by
        <a href="https://github.com/bilinearlabs/quixote" target="_blank">Bilinear Labs</a>
    </div>
    """,
    unsafe_allow_html=True
)
