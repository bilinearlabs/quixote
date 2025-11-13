"""
Etherduck Frontend - Streamlit Application
Fast and simple frontend for USDC Transfer events analytics
"""

import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from collections import Counter
import plotly.graph_objects as go
import plotly.express as px

# Configuration
API_BASE_URL = "http://localhost:9988"
CONTRACT_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
CONTRACT_NAME = "USDC"
EVENT_NAME = "Transfer(address, address, uint256)"

# Page config
st.set_page_config(
    page_title="Etherduck Dashboard",
    page_icon="🦆",
    layout="wide",
    initial_sidebar_state="collapsed"
)

@st.cache_data(ttl=60)  # Cache for 60 seconds
def fetch_events(start_time: datetime, end_time: datetime = None):
    """Fetch events from the API"""
    url = f"{API_BASE_URL}/get_events"
    
    start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ") if end_time else None
    
    payload = {
        "contract": CONTRACT_ADDRESS,
        "start_time": start_time_str,
        "end_time": end_time_str
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("events", [])
    except Exception as e:
        st.error(f"Error fetching events: {e}")
        return []

def get_events_by_hour(last_days: int = 7):
    """Get events grouped by hour"""
    end_time = datetime.now(timezone.utc).replace(tzinfo=None)
    start_time = end_time - timedelta(days=last_days)
    
    events = fetch_events(start_time, end_time)
    
    if not events:
        return pd.DataFrame(columns=["timestamp", "count"])
    
    # Parse timestamps and group by hour
    hourly_counts = {}
    for event in events:
        timestamp_str = event.get("block_timestamp")
        if timestamp_str:
            try:
                if timestamp_str.endswith('Z'):
                    event_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                else:
                    event_time = datetime.fromisoformat(timestamp_str)
                event_time = event_time.replace(tzinfo=None)
                
                # Floor to hour
                hour_key = event_time.replace(minute=0, second=0, microsecond=0)
                hourly_counts[hour_key] = hourly_counts.get(hour_key, 0) + 1
            except (ValueError, AttributeError):
                continue
    
    # Convert to DataFrame
    if not hourly_counts:
        return pd.DataFrame(columns=["timestamp", "count"])
    
    df = pd.DataFrame([
        {"timestamp": ts, "count": count}
        for ts, count in sorted(hourly_counts.items())
    ])
    
    return df

def get_top_addresses(last_hours: int = 24):
    """Get top 5 addresses with most transactions"""
    end_time = datetime.now(timezone.utc).replace(tzinfo=None)
    start_time = end_time - timedelta(hours=last_hours)
    
    events = fetch_events(start_time, end_time)
    
    if not events:
        return pd.DataFrame(columns=["address", "transaction_count"])
    
    topic1_counts = Counter()
    for event in events:
        timestamp_str = event.get("block_timestamp")
        if timestamp_str:
            try:
                if timestamp_str.endswith('Z'):
                    event_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                else:
                    event_time = datetime.fromisoformat(timestamp_str)
                event_time = event_time.replace(tzinfo=None)
                if not (start_time <= event_time <= end_time):
                    continue
            except (ValueError, AttributeError):
                pass
        
        topic1 = event.get("topic1")
        if topic1 and len(topic1) >= 42:
            address = "0x" + topic1[-40:].lower()
            topic1_counts[address] += 1
    
    top_5 = topic1_counts.most_common(5)
    return pd.DataFrame([
        {"address": addr, "transaction_count": count}
        for addr, count in top_5
    ])

def get_event_count_in_period(hours: float, data_start=None, data_end=None):
    """Get count of events in a time period"""
    if data_start and data_end:
        range_duration = (data_end - data_start).total_seconds() / 3600
        if hours <= range_duration:
            end_time = data_end
            start_time = data_end - timedelta(hours=hours)
        else:
            start_time = data_start
            end_time = data_end
    else:
        end_time = datetime.now(timezone.utc).replace(tzinfo=None)
        start_time = end_time - timedelta(hours=hours)
    
    events = fetch_events(start_time, end_time)
    
    if not events:
        return 0
    
    count = 0
    for event in events:
        timestamp_str = event.get("block_timestamp")
        if timestamp_str:
            try:
                if timestamp_str.endswith('Z'):
                    event_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                else:
                    event_time = datetime.fromisoformat(timestamp_str)
                event_time = event_time.replace(tzinfo=None)
                if start_time <= event_time <= end_time:
                    count += 1
            except (ValueError, AttributeError):
                count += 1
    
    return count

# Main app
st.title("🦆 Etherduck Dashboard")
st.markdown(f"### {CONTRACT_NAME} - {EVENT_NAME}")

# Refresh button
if st.button("🔄 Refresh Data", type="primary"):
    st.cache_data.clear()

# Get events data
with st.spinner("Loading events data..."):
    df_events = get_events_by_hour(7)

# Gauges
if not df_events.empty:
    data_start = df_events["timestamp"].min()
    data_end = df_events["timestamp"].max()
    
    # Get gauge values
    count_5min = get_event_count_in_period(5/60, data_start, data_end)
    count_30min = get_event_count_in_period(0.5, data_start, data_end)
    count_1h = get_event_count_in_period(1, data_start, data_end)
    data_range_hours = (data_end - data_start).total_seconds() / 3600
    gauge_24h_hours = min(24, data_range_hours) if data_range_hours > 0 else 24
    count_24h = get_event_count_in_period(gauge_24h_hours, data_start, data_end)
    
    def create_gauge_figure(value, title, max_value):
        """Create a gauge/indicator figure"""
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=value,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': title, 'font': {'size': 16}},
            gauge={
                'axis': {'range': [None, max_value]},
                'bar': {'color': "#1f77b4"},
                'steps': [
                    {'range': [0, max_value * 0.5], 'color': "lightgray"},
                    {'range': [max_value * 0.5, max_value * 0.8], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': max_value * 0.9
                }
            }
        ))
        fig.update_layout(
            height=250,
            margin=dict(l=20, r=20, t=40, b=20),
            paper_bgcolor="white",
            font={'color': "#333", 'family': "Arial"}
        )
        return fig
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        max_5min = max(count_5min * 1.5, 100) if count_5min > 0 else 100
        fig_5min = create_gauge_figure(count_5min, "Last 5 Minutes", max_5min)
        st.plotly_chart(fig_5min, use_container_width=True, config={'displayModeBar': False})
    
    with col2:
        max_30min = max(count_30min * 1.5, 500) if count_30min > 0 else 500
        fig_30min = create_gauge_figure(count_30min, "Last 30 Minutes", max_30min)
        st.plotly_chart(fig_30min, use_container_width=True, config={'displayModeBar': False})
    
    with col3:
        max_1h = max(count_1h * 1.5, 2000) if count_1h > 0 else 2000
        fig_1h = create_gauge_figure(count_1h, "Last 1 Hour", max_1h)
        st.plotly_chart(fig_1h, use_container_width=True, config={'displayModeBar': False})
    
    with col4:
        max_24h = max(count_24h * 1.2, 50000) if count_24h > 0 else 50000
        fig_24h = create_gauge_figure(count_24h, "Last 24 Hours", max_24h)
        st.plotly_chart(fig_24h, use_container_width=True, config={'displayModeBar': False})
    
    st.divider()
    
    # Events over time graph
    st.subheader("Events per Hour (Last 7 Days)")
    if not df_events.empty and df_events["count"].sum() > 0:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_events["timestamp"],
            y=df_events["count"],
            mode="lines+markers",
            name="Events per Hour",
            line=dict(color="#1f77b4", width=2),
            marker=dict(size=6, color="#1f77b4")
        ))
        fig.update_layout(
            height=400,
            xaxis_title="Time",
            yaxis_title="Number of Events",
            hovermode="x unified",
            template="plotly_white",
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No events data available for the selected time range.")
    
    st.divider()
    
    # Top addresses table
    st.subheader("Top 5 Addresses by Transaction Count (Last 24 Hours)")
    with st.spinner("Loading top addresses..."):
        df_top = get_top_addresses(24)
    
    if not df_top.empty:
        st.dataframe(
            df_top,
            use_container_width=True,
            hide_index=True,
            column_config={
                "address": st.column_config.TextColumn("Address", width="large"),
                "transaction_count": st.column_config.NumberColumn(
                    "Transaction Count",
                    format="%d"
                )
            }
        )
    else:
        st.info("No address data available for the last 24 hours.")
else:
    st.warning("No events data available. Please check your API connection and database.")

# Auto-refresh
st.markdown("---")
st.caption("Data refreshes automatically. Click 'Refresh Data' to force an update.")

