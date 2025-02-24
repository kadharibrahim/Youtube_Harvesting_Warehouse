import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
from googleapiclient.discovery import build
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ---------------------- Initialize YouTube API ----------------------
API_KEY = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=API_KEY)

# ---------------------- Database Connection ----------------------
def get_db_connection():
    try:
        return mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE")
        )
    except mysql.connector.Error as e:
        st.error(f"\u274C Database connection failed: {e}")
        return None

# ---------------------- Store Channel Data into MySQL ----------------------
def store_channel_data(channel_data):
    conn = get_db_connection()
    if not conn:
        return

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO channels (channel_id, channel_name, subscribers, views, total_videos, description, playlist_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                channel_name = VALUES(channel_name),
                subscribers = VALUES(subscribers),
                views = VALUES(views),
                total_videos = VALUES(total_videos),
                description = VALUES(description),
                playlist_id = VALUES(playlist_id)
            """, (
                channel_data["channel_id"],
                channel_data["channel_name"],
                channel_data["subscribers"],
                channel_data["views"],
                channel_data["total_videos"],
                channel_data["description"],
                channel_data["playlist_id"]
            ))
            conn.commit()
        st.success(f"\u2705 Data for {channel_data['channel_name']} stored successfully!")
    except Exception as e:
        st.error(f"\u274C Error storing channel data: {e}")
    finally:
        conn.close()

# ---------------------- Fetch YouTube Data ----------------------
def fetch_channel_data(channel_id):
    try:
        request = youtube.channels().list(
            part="snippet,statistics,contentDetails",
            id=channel_id
        )
        response = request.execute()
        
        if "items" in response and response["items"]:
            data = response["items"][0]
            return {
                "channel_id": channel_id,
                "channel_name": data["snippet"]["title"],
                "subscribers": data["statistics"].get("subscriberCount", 0),
                "views": data["statistics"].get("viewCount", 0),
                "total_videos": data["statistics"].get("videoCount", 0),
                "description": data["snippet"].get("description", ""),
                "playlist_id": data["contentDetails"]["relatedPlaylists"].get("uploads", "")
            }
        return None
    except Exception as e:
        st.error(f"\u274C Error fetching channel data: {e}")
        return None

# ---------------------- Fetch Data from MySQL ----------------------
def fetch_data(query, params=None):
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()  

    try:
        with conn.cursor(dictionary=True) as cursor:
            cursor.execute(query, params if params else ())
            result = cursor.fetchall()
            return pd.DataFrame(result)
    except Exception as e:
        st.error(f"\u274C Error executing query: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# ---------------------- Data Migration Function ----------------------
def migrate_data():
    conn = get_db_connection()
    if not conn:
        return

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO archived_videos (video_id, title, views, likes, comments)
                SELECT video_id, title, views, likes, comment_count FROM videos
                ON DUPLICATE KEY UPDATE
                views = VALUES(views),
                likes = VALUES(likes),
                comments = VALUES(comments)
            """)
            conn.commit()
        st.success("\u2705 Data migration completed successfully!")
    except Exception as e:
        st.error(f"\u274C Data migration failed: {e}")
    finally:
        conn.close()

# ---------------------- Streamlit UI ----------------------
st.markdown("""
    <div style="text-align: center;">
        <img src="https://upload.wikimedia.org/wikipedia/commons/b/b8/YouTube_Logo_2017.svg" alt="YouTube Logo" width="150">
        <h1>Harvesting & Warehousing</h1>
    </div>
""", unsafe_allow_html=True)


import streamlit as st

# ---------------------- Streamlit UI Layout ----------------------
st.sidebar.title("📌 Skills Takeaway from This Project")
st.sidebar.write("""
- 🐍 **Python Scripting**  
- 📊 **Data Collection**  
- 🎨 **Streamlit for UI**  
- 🔗 **API Integration** (YouTube API)  
- 🗄️ **Data Management using SQL**  
- 📊 **Data Visualization using Plotly & Matplotlib**  
""")

# User input for Channel ID
channel_id = st.text_input("🔎 Enter the Channel ID")

col1, col2 = st.columns(2)
with col1:
    if st.button("📥 Collect and Store Data"):
        if channel_id:
            channel_data = fetch_channel_data(channel_id)
            if channel_data:
                store_channel_data(channel_data)
            else:
                st.warning("⚠️ No data found for the given Channel ID.")
        else:
            st.warning("⚠️ Please enter a Channel ID.")

with col2:
    if st.button("🛠️ Migrate to SQL"):
        migrate_data()

# Fetch available channels
df_channels = fetch_data("SELECT channel_id, channel_name FROM channels")

# Select Channel Dropdown
channel_options = ["None"] + df_channels["channel_name"].tolist() if not df_channels.empty else ["None"]
selected_channel_name = st.selectbox("🔽 Select a Channel", channel_options)

# Get selected Channel ID
selected_channel_id = df_channels[df_channels["channel_name"] == selected_channel_name]["channel_id"].values[0] if selected_channel_name != "None" and not df_channels.empty else None

col3, col4 = st.columns(2)
with col3:
    if st.button("📥 Collect and Store Data for Selected Channel"):
        if selected_channel_id:
            channel_data = fetch_channel_data(selected_channel_id)
            if channel_data:
                store_channel_data(channel_data)
            else:
                st.warning("⚠️ No data found for the selected Channel.")
        else:
            st.warning("⚠️ Please select a channel.")

with col4:
    if st.button("🛠️ Migrate to SQL for Selected Channel"):
        migrate_data()

# ---------------------- Display Data ----------------------
active_channel_id = channel_id if channel_id else selected_channel_id

if active_channel_id:
    st.subheader(f"📌 Showing Data for Channel: {selected_channel_name if selected_channel_name != 'None' else channel_id}")

    st.write("### 📌 Channel Details")
    df_channel_data = fetch_data("SELECT * FROM channels WHERE channel_id = %s", (active_channel_id,))
    if not df_channel_data.empty:
        st.dataframe(df_channel_data)
    else:
        st.warning("⚠️ No channel found.")

    st.write("### 📌 Playlists from this Channel")
    df_playlists = fetch_data("SELECT * FROM playlists WHERE channel_id = %s", (active_channel_id,))
    if not df_playlists.empty:
        st.dataframe(df_playlists)
    else:
        st.warning("⚠️ No playlists found.")

    st.write("### 📌 Videos from this Channel")
    df_videos = fetch_data("SELECT * FROM videos WHERE channel_id = %s", (active_channel_id,))
    if not df_videos.empty:
        st.dataframe(df_videos)
    else:
        st.warning("⚠️ No videos found.")

    if not df_videos.empty:
        video_ids = tuple(df_videos["video_id"])
        if video_ids:
            st.write("### 📌 Comments on Videos from this Channel")
            placeholders = ", ".join(["%s"] * len(video_ids))
            query = f"SELECT * FROM comments WHERE video_id IN ({placeholders})"
            df_comments = fetch_data(query, video_ids)
            st.dataframe(df_comments if not df_comments.empty else st.warning("⚠️ No comments found."))
else:
    st.warning("⚠️ Please enter a Channel ID or select a channel.")

# ---------------------- SQL Queries for Insights ----------------------
QUERIES = {
    "Videos and their Channels": """
        SELECT v.title AS video_name, c.channel_name
        FROM videos v
        JOIN channels c ON v.channel_id = c.channel_id
    """,
 
    "Channels with Most Videos": """
        SELECT c.channel_name, COUNT(v.video_id) AS video_count
        FROM videos v
        JOIN channels c ON v.channel_id = c.channel_id
        GROUP BY c.channel_name
        ORDER BY video_count DESC
    """,
 
    "Top 10 Most Viewed Videos": """
        SELECT v.title AS video_name, c.channel_name, v.views
        FROM videos v
        JOIN channels c ON v.channel_id = c.channel_id
        ORDER BY v.views DESC
        LIMIT 10
    """,
 
    "Total Views per Channel": """
        SELECT c.channel_name, COALESCE(SUM(v.views), 0) AS total_views
        FROM channels c
        LEFT JOIN videos v ON c.channel_id = v.channel_id
        GROUP BY c.channel_name
    """,
 
    "Channels that Published Videos in 2022": """
        SELECT DISTINCT c.channel_name
        FROM videos v
        JOIN channels c ON v.channel_id = c.channel_id
        WHERE YEAR(v.published_date) = 2022
    """,
 
    "Average Video Duration per Channel": """
        SELECT
            c.channel_name,
            AVG(
                TIME_TO_SEC(
                    STR_TO_DATE(REPLACE(REPLACE(REPLACE(v.duration, 'PT', ''), 'M', ':'), 'S', ''), '%i:%s')
                )
            ) AS avg_duration_minutes
        FROM channels c
        JOIN videos v ON c.channel_id = v.channel_id
        WHERE v.duration IS NOT NULL AND v.duration <> ''
        GROUP BY c.channel_name
    """,
 
    "Top 10 Videos with Most Comments": """
        SELECT v.title AS video_name, c.channel_name, COUNT(cm.comment_id) AS comment_count
        FROM comments cm
        JOIN videos v ON cm.video_id = v.video_id
        JOIN channels c ON v.channel_id = c.channel_id
        GROUP BY v.title, c.channel_name
        ORDER BY comment_count DESC
        LIMIT 10
    """,
 
    "Videos with Most Likes": """
        SELECT v.title AS video_name, c.channel_name, v.likes
        FROM videos v
        JOIN channels c ON v.channel_id = c.channel_id
        ORDER BY v.likes DESC
        LIMIT 10
    """,
 
    "Total Likes per Video": """
        SELECT v.title AS video_name, v.likes
        FROM videos v
        ORDER BY v.likes DESC
    """,
 
    "Videos with Most Liked Comments": """
        SELECT v.title AS video_name, c.channel_name, SUM(cm.likes) AS total_comment_likes  
        FROM comments cm  
        JOIN videos v ON cm.video_id = v.video_id  
        JOIN channels c ON v.channel_id = c.channel_id  
        GROUP BY v.title, c.channel_name  
        ORDER BY total_comment_likes DESC  
        LIMIT 10
    """
}

 
import streamlit as st
import plotly.express as px

def fetch_data(query, params=None):
    # Placeholder function for database queries
    return ""  # Replace with actual database fetching logic

st.title("🔍 YouTube Data Insights")

query_option = st.selectbox("Select a query:", list(QUERIES.keys()))

if st.button("Run Query"):
    df = fetch_data(QUERIES[query_option])
    st.dataframe(df if not df.empty else st.warning("⚠️ No data found for this query."))

st.title("📊 Data Visualizations")

# Select Visualization Type
visualization_type = st.radio(
    "📌 Select a Visualization",
    ["None", "Total Views per Channel", "Top 10 Most Viewed Videos", "Average Video Duration per Channel", "Videos with Most Liked Comments"]
)

# 📊 **Total Views per Channel**
if visualization_type == "Total Views per Channel":
    st.write("### 📊 Total Views per Channel")
    df_views = fetch_data("""
        SELECT c.channel_name, COALESCE(SUM(v.views), 0) AS total_views
        FROM channels c
        LEFT JOIN videos v ON c.channel_id = v.channel_id
        GROUP BY c.channel_name
        ORDER BY total_views DESC
    """)
    if not df_views.empty:
        fig = px.bar(df_views, x="channel_name", y="total_views", title="Total Views per Channel", color="total_views", height=500)
        st.plotly_chart(fig)
    else:
        st.warning("⚠️ No data available.")

# 📊 **Top 10 Most Viewed Videos**
elif visualization_type == "Top 10 Most Viewed Videos":
    st.write("### 📊 Top 10 Most Viewed Videos")
    df_top_videos = fetch_data("""
        SELECT v.title AS video_name, c.channel_name, v.views
        FROM videos v
        JOIN channels c ON v.channel_id = c.channel_id
        ORDER BY v.views DESC
        LIMIT 10
    """)
    if not df_top_videos.empty:
        fig = px.bar(df_top_videos, x="views", y="video_name", title="Top 10 Most Viewed Videos", color="views", orientation="h", height=500)
        st.plotly_chart(fig)
    else:
        st.warning("⚠️ No data available.")

# ⏳ **Average Video Duration per Channel**
elif visualization_type == "Average Video Duration per Channel":
    st.write("### ⏳ Average Video Duration per Channel")
    df_avg_duration = fetch_data("""
        SELECT
            c.channel_name,
            AVG(
                TIME_TO_SEC(
                    STR_TO_DATE(REPLACE(REPLACE(REPLACE(v.duration, 'PT', ''), 'M', ':'), 'S', ''), '%i:%s')
                )
            ) / 60 AS avg_duration_minutes
        FROM channels c
        JOIN videos v ON c.channel_id = v.channel_id
        WHERE v.duration IS NOT NULL AND v.duration <> ''
        GROUP BY c.channel_name
    """)
    if not df_avg_duration.empty:
        fig = px.bar(df_avg_duration, x="channel_name", y="avg_duration_minutes", title="Average Video Duration (Minutes)", color="avg_duration_minutes", height=500)
        st.plotly_chart(fig)
    else:
        st.warning("⚠️ No data available.")

# ❤️ **Videos with Most Liked Comments**
elif visualization_type == "Videos with Most Liked Comments":
    st.write("### ❤️ Videos with Most Liked Comments")
    df_liked_comments = fetch_data("""
        SELECT v.title AS video_name, c.channel_name, SUM(cm.likes) AS total_comment_likes  
        FROM comments cm  
        JOIN videos v ON cm.video_id = v.video_id  
        JOIN channels c ON v.channel_id = c.channel_id  
        GROUP BY v.title, c.channel_name  
        ORDER BY total_comment_likes DESC  
        LIMIT 10
    """)
    if not df_liked_comments.empty:
        fig = px.pie(df_liked_comments, names="video_name", values="total_comment_likes", title="Videos with Most Liked Comments")
        st.plotly_chart(fig)
    else:
        st.warning("⚠️ No data available.")
