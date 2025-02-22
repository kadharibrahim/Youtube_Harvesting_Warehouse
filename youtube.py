from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st
import os
import mysql.connector
from mysql.connector import Error

# Secure API Key (Stored in Environment Variable)
API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# Initialize YouTube API
def api_connect():
    if not API_KEY:
        raise ValueError("Missing YouTube API Key. Set YOUTUBE_API_KEY as an environment variable.")
    return build(YOUTUBE_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

youtube = api_connect()

# Secure Database Connection
def db_connect():
    try:
        return mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            port=os.getenv("DB_PORT", "3306")
        )
    except Error as err:
        print(f"Database Connection Error: {err}")
        return None

# Fetch Channel Info
def get_channel_info(channel_id: str) -> dict:
    try:
        response = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        ).execute()

        if not response.get("items"):
            return {}

        item = response["items"][0]
        return {
            "Channel_Name": item["snippet"]["title"],
            "Channel_Id": item["id"],
            "Subscribers": int(item["statistics"].get("subscriberCount", 0)),
            "Views": int(item["statistics"].get("viewCount", 0)),
            "Total_Videos": int(item["statistics"].get("videoCount", 0)),
            "Channel_Description": item["snippet"].get("description", ""),
            "Playlist_Id": item["contentDetails"]["relatedPlaylists"]["uploads"]
        }
    except HttpError as e:
        print(f"API Error: {e}")
        return {}

# Fetch Video IDs
def get_video_ids(channel_id: str) -> list:
    try:
        response = youtube.channels().list(
            id=channel_id, part="contentDetails"
        ).execute()

        playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        video_ids, next_page_token = [], None

        while True:
            response = youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            video_ids.extend(item["snippet"]["resourceId"]["videoId"] for item in response.get("items", []))
            next_page_token = response.get("nextPageToken")

            if not next_page_token:
                break

        return video_ids
    except HttpError as e:
        print(f"API Error: {e}")
        return []

# Fetch Video Info
def get_video_info(video_ids: list) -> list:
    video_data = []
    for video_id in video_ids:
        try:
            response = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            ).execute()

            for item in response.get("items", []):
                video_data.append({
                    "Channel_Name": item["snippet"]["channelTitle"],
                    "Channel_Id": item["snippet"]["channelId"],
                    "Video_Id": item["id"],
                    "Title": item["snippet"]["title"],
                    "Tags": item["snippet"].get("tags", []),
                    "Thumbnail": item["snippet"]["thumbnails"],
                    "Description": item["snippet"].get("description", ""),
                    "Published_Date": item["snippet"]["publishedAt"],
                    "Duration": item["contentDetails"]["duration"],
                    "Views": int(item["statistics"].get("viewCount", 0)),
                    "Comments": int(item["statistics"].get("commentCount", 0)),
                    "Favorite_Count": int(item["statistics"].get("favoriteCount", 0)),
                    "Definition": item["contentDetails"]["definition"],
                    "Caption_Status": item["contentDetails"]["caption"]
                })
        except HttpError as e:
            print(f"API Error for video {video_id}: {e}")

    return video_data

# Fetch Comments
def get_comment_info(video_ids: list) -> list:
    comments_data = []
    for video_id in video_ids:
        try:
            response = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            ).execute()

            comments_data.extend({
                "Comment_Id": item["snippet"]["topLevelComment"]["id"],
                "Video_Id": item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                "Comment_Text": item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                "Comment_Authors": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                "Comment_Published": item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
            } for item in response.get("items", []))
        except HttpError as e:
            print(f"API Error for video {video_id}: {e}")

    return comments_data

# Fetch Playlist Details
def get_playlist_details(channel_id: str) -> list:
    playlists = []
    next_page_token = None

    try:
        while True:
            response = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            playlists.extend({
                "Playlist_Id": item["id"],
                "Title": item["snippet"]["title"],
                "Channel_Id": item["snippet"]["channelId"],
                "Channel_Name": item["snippet"]["channelTitle"],
                "Published_At": item["snippet"]["publishedAt"],
                "Video_Count": item["contentDetails"]["itemCount"]
            } for item in response.get("items", []))

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
    except HttpError as e:
        print(f"API Error: {e}")

    return playlists

# Create Tables in Database
def create_tables():
    db_connection = db_connect()
    if not db_connection:
        return
    
    cursor = db_connection.cursor()

    try:
        table_queries = [
            """
            CREATE TABLE IF NOT EXISTS channels (
                channel_id VARCHAR(255) PRIMARY KEY,
                channel_name VARCHAR(255) NOT NULL,
                subscribers INT DEFAULT 0,
                views BIGINT DEFAULT 0,
                total_videos INT DEFAULT 0,
                description TEXT NULL,
                playlist_id VARCHAR(255) NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS videos (
                video_id VARCHAR(255) PRIMARY KEY,
                channel_id VARCHAR(255) NOT NULL,
                title VARCHAR(255) NOT NULL,
                tags TEXT NULL,
                thumbnail TEXT NULL,
                description TEXT NULL,
                published_date DATETIME NOT NULL,
                duration INT DEFAULT 0,
                views BIGINT DEFAULT 0,
                comment_count INT DEFAULT 0,
                favorite_count INT DEFAULT 0,
                definition ENUM('hd', 'sd') NOT NULL,
                caption_status ENUM('true', 'false') NOT NULL,
                FOREIGN KEY (channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS comments (
                comment_id VARCHAR(255) PRIMARY KEY,
                video_id VARCHAR(255) NOT NULL,
                comment_text TEXT NOT NULL,
                comment_author VARCHAR(255) NOT NULL,
                published_date DATETIME NOT NULL,
                FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS playlists (
                playlist_id VARCHAR(255) PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                channel_id VARCHAR(255) NOT NULL,
                channel_name VARCHAR(255) NOT NULL,
                published_at DATETIME NOT NULL,
                video_count INT DEFAULT 0,
                FOREIGN KEY (channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE
            );
            """
        ]

        for query in table_queries:
            cursor.execute(query)
        
        db_connection.commit()
        print("✅ Tables created successfully!")

    except Error as err:
        print(f"❌ Error executing query: {err}")
    
    finally:
        cursor.close()
        db_connection.close()

if __name__ == "__main__":
    create_tables()


import os
import mysql.connector
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import datetime
import isodate
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Fetch API Keys and Database Credentials
api_keys = os.getenv("YOUTUBE_API_KEYS").split(",")
db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": os.getenv("DB_PORT"),
}

# Function to connect to MySQL
def get_db_connection():
    try:
        return mysql.connector.connect(**db_config)
    except mysql.connector.Error as err:
        print(f"Database Connection Error: {err}")
        return None

# Function to fetch videos from YouTube API
def get_videos(channel_id):
    videos = []
    for api_key in api_keys:
        try:
            youtube = build("youtube", "v3", developerKey=api_key)

            request = youtube.search().list(
                part="id",
                channelId=channel_id,
                maxResults=50,
                type="video"
            )
            response = request.execute()

            for item in response.get("items", []):
                video_id = item["id"]["videoId"]
                video_details = get_video_details(youtube, video_id)
                if video_details:
                    videos.append(video_details)
            
            if videos:
                return videos  # ✅ Return videos if data is fetched successfully

        except HttpError as e:
            if e.resp.status == 403:  # ✅ API quota exceeded
                print(f"API Key {api_key} quota exceeded. Trying the next API key...")
                continue  # ✅ Try with the next API key
            else:
                print(f"Error fetching videos: {e}")
                return []

    print("All API keys exceeded quota or failed.")
    return []

# Function to fetch video details
def get_video_details(youtube, video_id):
    try:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        if not response["items"]:
            return None

        item = response["items"][0]
        snippet = item["snippet"]
        content_details = item["contentDetails"]
        statistics = item.get("statistics", {})

        # Extract Required Data
        channel_id = snippet["channelId"]
        title = snippet["title"]
        tags = ", ".join(snippet.get("tags", []))  # ✅ Convert list to comma-separated string
        thumbnail = snippet["thumbnails"]["high"]["url"]
        description = snippet.get("description", "")

        # ✅ Convert Published Date Format
        published_at = snippet["publishedAt"]
        try:
            published_date = datetime.datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            published_date = datetime.datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
        published_date = published_date.strftime('%Y-%m-%d %H:%M:%S')

        # ✅ Fix Duration - Convert ISO 8601 to Seconds
        duration_iso = content_details["duration"]
        duration_seconds = int(isodate.parse_duration(duration_iso).total_seconds())

        # ✅ Fix Missing View Counts - Default to 0
        views = int(statistics.get("viewCount", 0))
        comments = int(statistics.get("commentCount", 0))
        favorite_count = int(statistics.get("favoriteCount", 0))
        definition = content_details["definition"]
        caption_status = content_details["caption"]

        return (video_id, channel_id, title, tags, thumbnail, description, published_date, duration_seconds, 
                views, comments, favorite_count, definition, caption_status)
    
    except HttpError as e:
        print(f"Error fetching video details: {e}")
        return None

# Function to insert videos into MySQL
def insert_videos(videos):
    conn = get_db_connection()
    if not conn:
        return

    cursor = conn.cursor()

    insert_query = """
        INSERT INTO videos (video_id, channel_id, title, tags, thumbnail, description, published_date, 
                            duration, views, comment_count, favorite_count, definition, caption_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            title=VALUES(title), tags=VALUES(tags), thumbnail=VALUES(thumbnail), 
            description=VALUES(description), published_date=VALUES(published_date), 
            duration=VALUES(duration), views=VALUES(views), comment_count=VALUES(comment_count),
            favorite_count=VALUES(favorite_count), definition=VALUES(definition),
            caption_status=VALUES(caption_status)
    """

    try:
        cursor.executemany(insert_query, videos)
        conn.commit()
        print("✅ Videos inserted successfully!")
    except mysql.connector.Error as err:
        print(f"❌ Error inserting videos: {err}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Function to fetch and insert channels data
def insert_channel_data(channel_data):
    conn = get_db_connection()
    if not conn:
        return

    cursor = conn.cursor()

    try:
        query = """
            INSERT INTO channels (channel_id, channel_name, subscribers, views, total_videos, description, playlist_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                channel_name = VALUES(channel_name),
                subscribers = VALUES(subscribers),
                views = VALUES(views),
                total_videos = VALUES(total_videos),
                description = VALUES(description),
                playlist_id = VALUES(playlist_id);
        """
        
        cursor.execute(query, (
            channel_data["Channel_Id"],
            channel_data["Channel_Name"],
            channel_data.get("Subscribers", 0),
            channel_data.get("views", 0),
            channel_data.get("Total_Videos", 0),
            channel_data.get("Channel_description", ""),
            channel_data.get("Playlist_Id", "")
        ))

        conn.commit()
        print(f"✅ Channel '{channel_data['Channel_Name']}' inserted/updated successfully.")

    except mysql.connector.Error as err:
        conn.rollback()
        print(f"❌ Database error inserting channel '{channel_data['Channel_Name']}': {err}")
    finally:
        cursor.close()
        conn.close()

# Test with a Sample Channel ID
if __name__ == "__main__":
    channel_id = "UC-mi8xUqL43BMlhvJbAf-Ew"
    videos = get_videos(channel_id)
    if videos:
        insert_videos(videos)

import mysql.connector
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
from config import API_KEYS, DB_CONFIG  # Import credentials from config.py

# MySQL connection setup
def create_connection():
    return mysql.connector.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"]
    )

# Convert YouTube datetime to MySQL datetime format
def convert_to_mysql_datetime(yt_datetime):
    try:
        return datetime.strptime(yt_datetime, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error converting datetime: {e}")
        return None

# Function to fetch video comments for a given video ID
def get_video_comments(video_id):
    comments = []
    
    for api_key in API_KEYS:
        try:
            youtube = build('youtube', 'v3', developerKey=api_key)
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            if 'items' in response:
                for item in response['items']:
                    comment = {
                        'Comment_Id': item['id'],
                        'Video_Id': video_id,
                        'Comment_Text': item['snippet']['topLevelComment']['snippet'].get('textDisplay', 'No text available'),
                        'Author': item['snippet']['topLevelComment']['snippet'].get('authorDisplayName', 'Unknown'),
                        'Published_Date': convert_to_mysql_datetime(item['snippet']['topLevelComment']['snippet']['publishedAt']),
                        'Likes': item['snippet']['topLevelComment']['snippet']['likeCount']
                    }
                    comments.append(comment)
            
            if comments:
                return comments
            else:
                print(f"No comments found for video {video_id}.")
                return None

        except HttpError as e:
            if e.resp.status == 403:  # Quota Exceeded error
                print(f"API Key {api_key} quota exceeded. Trying the next API key...")
                continue  # Try with the next API key
            else:
                print(f"Error fetching comments for video {video_id}: {e}")
                return None

    # If all API keys failed or quota exceeded
    print(f"All API keys exceeded quota or failed for video {video_id}.")
    return None

# Function to insert comment data into MySQL
def insert_comment_data(connection, comment_data):
    try:
        cursor = connection.cursor()

        # Prepare comment data for insertion into the database
        comment_data_list = [(comment['Comment_Id'], comment['Video_Id'], comment['Comment_Text'],
                              comment['Author'], comment['Published_Date'], comment['Likes'])
                             for comment in comment_data]

        # Insert comment data into the database
        cursor.executemany("""
            INSERT INTO comments (comment_id, video_id, comment_text, comment_author, published_date, likes)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, comment_data_list)

        connection.commit()
        print(f"Successfully inserted {len(comment_data)} comments.")

    except Exception as e:
        connection.rollback()
        print(f"Error inserting comment data: {e}")
    
    finally:
        cursor.close()

# Function to fetch video IDs for a given channel
def get_video_ids(api_key, channel_id):
    youtube = build("youtube", "v3", developerKey=api_key)
    video_ids = []
    next_page_token = None

    while True:
        request = youtube.search().list(
            part="id",
            channelId=channel_id,
            maxResults=50,
            type="video",
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response.get("items", []):
            video_ids.append(item["id"]["videoId"])

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return video_ids

CHANNEL_ID = "UC-mi8xUqL43BMlhvJbAf-Ew"  # Replace with the desired channel ID
video_ids = get_video_ids(API_KEYS[0], CHANNEL_ID)
connection = create_connection()

for video_id in video_ids:
    print(f"Fetching comments for video {video_id}...")
    comments = get_video_comments(video_id)
    if comments:
        insert_comment_data(connection, comments)
    else:
        print(f"No comments found for video {video_id}.")

connection.close()

import datetime
import mysql.connector
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from config import API_KEYS, DB_CONFIG  # Import credentials from config.py

# MySQL connection setup
def create_connection():
    return mysql.connector.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"]
    )

# Function to get playlists from a YouTube channel
def get_playlists(channel_id):
    playlists = []
    
    for api_key in API_KEYS:
        try:
            youtube = build('youtube', 'v3', developerKey=api_key)
            
            request = youtube.playlists().list(
                part="snippet",
                channelId=channel_id,
                maxResults=50
            )
            while request:
                response = request.execute()
                for item in response["items"]:
                    playlist_id = item["id"]
                    title = item["snippet"]["title"]
                    channel_id = item["snippet"]["channelId"]
                    channel_name = item["snippet"]["channelTitle"]
                    published_at_str = item["snippet"].get("publishedAt", None)

                    if published_at_str:
                        try:
                            published_at = datetime.datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                        except ValueError:
                            published_at = datetime.datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%SZ")
                        
                        published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        published_at = None
                
                    video_count = item["snippet"].get("itemCount", None)
                    
                    playlists.append({
                        "playlist_id": playlist_id,
                        "title": title,
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "published_at": published_at,
                        "video_count": video_count
                    })
                request = youtube.playlists().list_next(request, response)
            
            if playlists:
                return playlists

        except HttpError as e:
            if e.resp.status == 403:
                print(f"API Key {api_key} quota exceeded. Trying the next API key...")
                continue  
            else:
                print(f"Error fetching playlists for channel {channel_id}: {e}")
                return []

    print(f"All API keys exceeded quota or failed for channel {channel_id}.")
    return []

# Function to insert playlist data into MySQL
def insert_playlist_data(connection, playlists):
    cursor = connection.cursor()
    
    for playlist in playlists:
        playlist_id = playlist["playlist_id"]
        title = playlist["title"]
        channel_id = playlist["channel_id"]
        channel_name = playlist["channel_name"]
        published_at = playlist["published_at"]
        video_count = playlist["video_count"] if playlist["video_count"] is not None else 0  # Default to 0 if None
        
        insert_query = """
        INSERT INTO playlists (playlist_id, title, channel_id, channel_name, published_at, video_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        try:
            cursor.execute(insert_query, (playlist_id, title, channel_id, channel_name, published_at if published_at else None, video_count))
            connection.commit()
        except mysql.connector.Error as err:
            print(f"Error inserting playlist {title}: {err}")
            connection.rollback()

    cursor.close()

# Main execution
def main():
    connection = create_connection()
    channel_ids = ["UC-mi8xUqL43BMlhvJbAf-Ew"]  # Example channel ID(s)

    for channel_id in channel_ids:
        playlists = get_playlists(channel_id)
        if playlists:
            insert_playlist_data(connection, playlists)

    connection.close()
    print("Playlist data inserted successfully!")

if __name__ == "__main__":
    main()


import streamlit as st
import mysql.connector
import pandas as pd
from googleapiclient.discovery import build
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# ---------------------- Initialize YouTube API ----------------------
API_KEY = os.getenv("API_KEY")
youtube = build("youtube", "v3", developerKey=API_KEY)

# ---------------------- Database Connection ----------------------
def get_db_connection():
    try:
        return mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
    except mysql.connector.Error as e:
        st.error(f"❌ Database connection failed: {e}")
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
        st.success(f"✅ Data for {channel_data['channel_name']} stored successfully!")
    except Exception as e:
        st.error(f"❌ Error storing channel data: {e}")
    finally:
        conn.close()

# ---------------------- Fetch YouTube Data ----------------------
def fetch_channel_data(channel_id):
    try:
        request = youtube.channels().list(
            part="snippet,statistics",
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
                "playlist_id": data.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads", "")
            }
        return None
    except Exception as e:
        st.error(f"❌ Error fetching channel data: {e}")
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
        st.error(f"❌ Error executing query: {e}")
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
        st.success("✅ Data migration completed successfully!")
    except Exception as e:
        st.error(f"❌ Data migration failed: {e}")
    finally:
        conn.close()

# ---------------------- Streamlit UI Layout ----------------------
st.title("📊 YouTube Data Harvesting & Warehousing")

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
    st.dataframe(df_channel_data if not df_channel_data.empty else st.warning("⚠️ No channel found."))

    st.write("### 📌 Playlists from this Channel")
    df_playlists = fetch_data("SELECT * FROM playlists WHERE channel_id = %s", (active_channel_id,))
    st.dataframe(df_playlists if not df_playlists.empty else st.warning("⚠️ No playlists found."))

    st.write("### 📌 Videos from this Channel")
    df_videos = fetch_data("SELECT * FROM videos WHERE channel_id = %s", (active_channel_id,))
    st.dataframe(df_videos if not df_videos.empty else st.warning("⚠️ No videos found."))

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
 
# ---------------------- Run SQL Queries from Dropdown ----------------------
st.title("🔍 YouTube Data Insights")
 
query_option = st.selectbox("Select a query:", list(QUERIES.keys()))
 
if st.button("Run Query"):
    df = fetch_data(QUERIES[query_option])
    st.dataframe(df if not df.empty else st.warning("⚠️ No data found for this query."))