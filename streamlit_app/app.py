import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
from datetime import datetime


conn = snowflake.connector.connect(
    account = "RHNRPLP-WF19033",
    user = "PRATHIK2025",
    password = "Prathik@snowflake@2025",
    role = "ACCOUNTADMIN",
    warehouse = "COMPUTE_WH",
    database = "SPOTIFY_WRAPPED",
    schema = "STREAMING_DATA"
)


# Query example
cur = conn.cursor()
try:
    cur.execute("SELECT * FROM V_TOP_ARTISTS")
    data = cur.fetchone()
    for row in data:
        print(row)
finally:
    cur.close()



# --- PAGE CONFIG ---
st.set_page_config(page_title="Spotify Wrapped 360", page_icon="🎵", layout="wide")

# --- CUSTOM CSS for Spotify vibes ---
st.markdown("""
    <style>
    body {
        background-color: #121212;
        color: white;
    }
    .stApp {
        background-color: #121212;
        border: 4px solid #1DB954;
        border-radius: 15px;
        padding: 20px;
        margin: 10px;
    }
    .block-container {
        padding-top: 2rem;
    }
    h1, h2, h3, h4 {
        color: #1DB954;
    }
    .metric-label {
        color: white;
    }
    footer {
        visibility: hidden;
    }
    .css-1v0mbdj {  
        padding: 1rem; 
        border-radius: 10px; 
        background-color: #191414; 
    }
    </style>
""", unsafe_allow_html=True)




# --- HERO SECTION ---
st.title("Spotify Wrapped 360 🎵")
st.markdown("#### Personalized Music Analytics Dashboard")
st.markdown("---")


# --- PULL DATA ---
df = pd.read_sql("""
    SELECT 
        TS, PLATFORM, MS_PLAYED, CONN_COUNTRY, IP_ADDR,
        MASTER_METADATA_TRACK_NAME, MASTER_METADATA_ALBUM_ARTIST_NAME, MASTER_METADATA_ALBUM_ALBUM_NAME,
        SPOTIFY_TRACK_URI, REASON_START, REASON_END, SHUFFLE, SKIPPED, OFFLINE, INCOGNITO_MODE
    FROM STREAMING_HISTORY_RAW_DATA
""", conn)

# --- CLEANING ---
df['TS'] = pd.to_datetime(df['TS'])

# --- KPIs ---
col1, col2, col3, col4 = st.columns(4)

# KPI Calculations
total_minutes = df['MS_PLAYED'].sum() / 60000
favorite_artist = df['MASTER_METADATA_ALBUM_ARTIST_NAME'].value_counts().idxmax()
favorite_platform = df['PLATFORM'].value_counts().idxmax()
peak_month = df['TS'].dt.to_period('M').value_counts().idxmax()

# KPI Display
col1.metric("🎵 Total Listening Hours", f"{round(total_minutes/60, 1)} hrs")
col2.metric("🎶 Favorite Artist", favorite_artist)
col3.metric("🎧 Favorite Platform", favorite_platform)
col4.metric("📅 Peak Month", str(peak_month))








st.markdown("---")
st.subheader("🎤 Top 10 Artists by Listening Time")

# Query top artists
top_artists = pd.read_sql("""
    SELECT
        MASTER_METADATA_ALBUM_ARTIST_NAME AS ARTIST_NAME,
        SUM(MS_PLAYED) / 60000 AS MINUTES_PLAYED
    FROM STREAMING_HISTORY_RAW_DATA
    GROUP BY MASTER_METADATA_ALBUM_ARTIST_NAME
    ORDER BY MINUTES_PLAYED DESC
    LIMIT 10
""", conn)

# Plot horizontal bar chart
fig_top_artists = px.bar(
    top_artists,
    x='MINUTES_PLAYED',
    y='ARTIST_NAME',
    orientation='h',
    labels={'MINUTES_PLAYED': 'Minutes Played', 'ARTIST_NAME': 'Artist'},
    title="Top 10 Artists"
)

# Flip to show most played artist at top
fig_top_artists.update_layout(yaxis=dict(autorange="reversed"))

# Show in Streamlit
st.plotly_chart(fig_top_artists, use_container_width=True)








st.markdown("---")
st.subheader("📈 Monthly Listening Trend")

# Query monthly listening trend
monthly_trend = pd.read_sql("""
    SELECT
        DATE_TRUNC('MONTH', TS) AS MONTH,
        SUM(MS_PLAYED) / 3600000 AS TOTAL_HOURS
    FROM STREAMING_HISTORY_RAW_DATA
    GROUP BY MONTH
    ORDER BY MONTH
""", conn)

# Convert 'MONTH' column to datetime if needed
monthly_trend['MONTH'] = pd.to_datetime(monthly_trend['MONTH'])

# Plot line chart
fig_monthly = px.line(
    monthly_trend,
    x='MONTH',
    y='TOTAL_HOURS',
    labels={'MONTH': 'Month', 'TOTAL_HOURS': 'Hours Listened'},
    title="Monthly Listening Hours",
    markers=True
)

# Show in Streamlit
st.plotly_chart(fig_monthly, use_container_width=True)







st.markdown("---")
st.subheader("📊 Platform Usage Distribution (Top 5 + Others)")

# Query platform usage
platform_usage = pd.read_sql("""
    SELECT
        PLATFORM,
        SUM(MS_PLAYED) / 3600000 AS TOTAL_HOURS
    FROM STREAMING_HISTORY_RAW_DATA
    GROUP BY PLATFORM
    ORDER BY TOTAL_HOURS DESC
""", conn)

# Process Top 5 + Others
top5 = platform_usage.head(5)
others = platform_usage.iloc[5:]

# Sum up "Others"
others_sum = others['TOTAL_HOURS'].sum()

# Create new DataFrame
platform_clean = top5.copy()
platform_clean.loc[len(platform_clean.index)] = ['Others', others_sum]

# Plot pie chart
fig_platform = px.pie(
    platform_clean,
    names='PLATFORM',
    values='TOTAL_HOURS',
    title="Listening Time by Platform (Top 5 + Others)",
    hole=0.4
)

# Show in Streamlit
st.plotly_chart(fig_platform, use_container_width=True)






# --- Section Divider
st.markdown("---")
st.subheader("📈 Actual vs Forecasted Monthly Listening Hours")

# --- 1. Load Actual Historical Data (from Snowflake)
historical = pd.read_sql("""
    SELECT
        DATE_TRUNC('MONTH', TS) AS MONTH,
        SUM(MS_PLAYED)/3600000 AS TOTAL_HOURS
    FROM STREAMING_HISTORY_RAW_DATA
    GROUP BY MONTH
    ORDER BY MONTH
""", conn)

historical['MONTH'] = pd.to_datetime(historical['MONTH'])
historical['Type'] = "Actual"

# --- 2. Hardcoded Forecast Data
forecast_data = {
    'MONTH': [
        '2025-04-01', '2025-05-01', '2025-06-01', '2025-07-01', '2025-08-01',
        '2025-09-01', '2025-10-01', '2025-11-01', '2025-12-01',
        '2026-01-01', '2026-02-01', '2026-03-01'
    ],
    'TOTAL_HOURS': [
        34.9582317248278, 22.60264767590734, 23.72874023074874, 20.04017724639444,
        27.684818267336713, 23.067825195626057, 28.22278909197485, 23.484938429445425,
        25.236307699516857, 23.388763328995292, 23.385483429432227, 21.654530150631626
    ]
}
forecast = pd.DataFrame(forecast_data)
forecast['MONTH'] = pd.to_datetime(forecast['MONTH'])
forecast['Type'] = "Forecast"

# --- 3. Combine Actual + Forecast
combined = pd.concat([historical, forecast])

# --- 4. Plot
fig_combined = px.line(
    combined,
    x='MONTH',
    y='TOTAL_HOURS',
    color='Type',  # Different colors for Actual vs Forecast
    line_dash='Type',  # Different line styles
    labels={'MONTH': 'Month', 'TOTAL_HOURS': 'Listening Hours'},
    title="Monthly Spotify Listening Hours: Actual vs Forecast",
    markers=True
)

fig_combined.update_layout(
    xaxis_title="Month",
    yaxis_title="Listening Hours",
    hovermode="x unified",
    template="plotly_white"
)

# --- 5. Display Plot
st.plotly_chart(fig_combined, use_container_width=True)



# Convert 'TS' to datetime if needed
df['TS'] = pd.to_datetime(df['TS'])






# --- Section Divider
st.markdown("---")
st.subheader("🚀 Track Skipping Behavior")

# Calculate Skip %
skip_rate = df['SKIPPED'].mean() * 100

col1, col2 = st.columns(2)
col1.metric("Skip Rate (%)", f"{skip_rate:.2f}%")
col2.metric("Completion Rate (%)", f"{100 - skip_rate:.2f}%")

# Plot Skip over Time
skip_monthly = df.copy()
skip_monthly['MONTH'] = skip_monthly['TS'].dt.to_period('M').dt.to_timestamp()
skip_summary = skip_monthly.groupby('MONTH')['SKIPPED'].mean().reset_index()
skip_summary['Skip_Rate_Percent'] = skip_summary['SKIPPED'] * 100

fig_skip = px.line(
    skip_summary,
    x='MONTH',
    y='Skip_Rate_Percent',
    title="Monthly Skip Rate Trend",
    labels={'Skip_Rate_Percent': 'Skip Rate (%)', 'MONTH': 'Month'},
    markers=True
)
fig_skip.update_layout(template="plotly_white", hovermode="x unified")

st.plotly_chart(fig_skip, use_container_width=True)










# --- Section Divider
st.markdown("---")
st.subheader("🎶 Shuffle and Offline Listening Trends")

# Prepare Monthly Data
shuffle_offline = df.copy()
shuffle_offline['MONTH'] = shuffle_offline['TS'].dt.to_period('M').dt.to_timestamp()

shuffle_summary = shuffle_offline.groupby('MONTH').agg({
    'SHUFFLE': 'mean',
    'OFFLINE': 'mean'
}).reset_index()

shuffle_summary['Shuffle (%)'] = shuffle_summary['SHUFFLE'] * 100
shuffle_summary['Offline (%)'] = shuffle_summary['OFFLINE'] * 100

# --- Separate Plots
st.markdown("##### 🎯 Separate Charts")

fig_shuffle = px.line(
    shuffle_summary,
    x='MONTH',
    y='Shuffle (%)',
    title="Monthly Shuffle Usage",
    markers=True
)
fig_shuffle.update_layout(template="plotly_white", hovermode="x unified")

fig_offline = px.line(
    shuffle_summary,
    x='MONTH',
    y='Offline (%)',
    title="Monthly Offline Listening Usage",
    markers=True
)
fig_offline.update_layout(template="plotly_white", hovermode="x unified")

st.plotly_chart(fig_shuffle, use_container_width=True)
st.plotly_chart(fig_offline, use_container_width=True)











# --- Section Divider
st.markdown("---")
st.subheader("🎧 How Do You Start Listening? (Reason Start)")

# Analyze Reason Start
reason_counts = df['REASON_START'].value_counts().reset_index()
reason_counts.columns = ['Reason_Start', 'Count']

fig_reason = px.pie(
    reason_counts,
    names='Reason_Start',
    values='Count',
    title="Start Reason Distribution",
    hole=0.4
)
fig_reason.update_traces(textinfo='percent+label')
fig_reason.update_layout(template="plotly_white")

st.plotly_chart(fig_reason, use_container_width=True)













# --- Section Divider
st.markdown("---")
st.subheader("💖 Listening Wellness Index")

# Calculate Listening Wellness
wellness = df.copy()
wellness['MONTH'] = wellness['TS'].dt.to_period('M').dt.to_timestamp()

monthly_summary = wellness.groupby('MONTH').agg({
    'SKIPPED': 'mean',
    'SHUFFLE': 'mean',
    'MS_PLAYED': 'mean'
}).reset_index()

# Wellness Score Formula
monthly_summary['Wellness_Score'] = (1 - monthly_summary['SKIPPED']) * monthly_summary['MS_PLAYED'] * (1 + monthly_summary['SHUFFLE'])

fig_wellness = px.line(
    monthly_summary,
    x='MONTH',
    y='Wellness_Score',
    title="Monthly Listening Wellness Index",
    labels={'MONTH': 'Month', 'Wellness_Score': 'Wellness Score'},
    markers=True
)
fig_wellness.update_layout(template="plotly_white", hovermode="x unified")

st.plotly_chart(fig_wellness, use_container_width=True)





# --- FOOTER ---
st.markdown("""
    <hr style='border: 0.5px solid #1DB954;'>
    <center style='color:white;'>
    Made with ❤️ by <b>Prathik Ravichandran</b> | Spotify Wrapped 360 | 2025
    </center>
""", unsafe_allow_html=True)

# --- END OF APP ---








