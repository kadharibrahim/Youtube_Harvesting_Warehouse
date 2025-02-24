from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st
import os

# Secure API Key (Store in Environment Variable)
API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# Initialize YouTube API
def api_connect():
    if not API_KEY:
        raise ValueError("Missing YouTube API Key. Set it as an environment variable.")
    return build(YOUTUBE_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

youtube = api_connect()

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


import os
import mysql.connector
from mysql.connector import Error

# Secure Database Connection
def db_connect():
    try:
        return mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME", "ibi"),
            port=os.getenv("DB_PORT", "3306")
        )
    except Error as err:
        print(f"Database Connection Error: {err}")
        return None

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
                duration INT DEFAULT 0, -- Store in seconds for easier calculations
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

channel_ids = [
    "UC2J_VKrAzOEJuQvFFtj3KUw", # CSK
    "UCl23mvQ3321L7zO6JyzhVmg", # MI
    "UCp10aBPqcOeBbEg7d_K9SBw", # KKR
    "UCkpgyRmcNy-aZFLUkKkWK4w", # RR
    "UCCBe9iIoN9Ar-Elluxca-Xw", # GT
    "UCEzB47eM-HZu04f4mB2nycg", # DC
    "UC-mi8xUqL43BMlhvJbAf-Ew", # LSG
    "UCCq1xDJMBRF61kiOgU90_kw", # RCB
    "UCScgEv0U9Wcnk24KfAzGTXg", # SRH
    "UCvRa1LWA_-aARq1AQMC4AyA"  # PBK
]

# Function to insert or update channel data
def insert_channel_data(channel_data):
    connection = db_connect()
    if not connection:
        return

    cursor = connection.cursor()

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

        connection.commit()
        print(f"✅ Channel '{channel_data['Channel_Name']}' inserted/updated successfully.")

    except mysql.connector.Error as err:
        connection.rollback()
        print(f"❌ Database error inserting channel '{channel_data['Channel_Name']}': {err}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        cursor.close()
        connection.close()


import mysql.connector
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import datetime
import isodate  # Fix for duration conversion
import os

# YouTube API Keys (Use Multiple to Avoid Quota Issues)
api_keys = os.getenv("YOUTUBE_API_KEYS", "").split(',')

# MySQL Connection
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "ibi"),
        port=os.getenv("DB_PORT", "3306")
    )

# Function to Fetch Videos from YouTube API
def get_videos(channel_id):
    videos = []
    for api_key in api_keys:
        try:
            youtube = build('youtube', 'v3', developerKey=api_key)

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
                return videos  # Return videos if data is fetched successfully

        except HttpError as e:
            if e.resp.status == 403:  # API quota exceeded
                print(f"API Key quota exceeded. Trying the next API key...")
                continue  # Try with the next API key
            else:
                print(f"Error fetching videos: {e}")
                return []

    print("All API keys exceeded quota or failed.")
    return []

# Function to Fetch Video Details
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
        tags = ", ".join(snippet.get("tags", []))  # Convert list to comma-separated string
        thumbnail = snippet["thumbnails"]["high"]["url"]
        description = snippet.get("description", "")
        
        # Convert Published Date Format
        published_at = snippet["publishedAt"]
        try:
            published_date = datetime.datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            published_date = datetime.datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
        published_date = published_date.strftime('%Y-%m-%d %H:%M:%S')

        # Convert Duration - Convert ISO 8601 to Seconds
        duration_iso = content_details["duration"]
        duration_seconds = int(isodate.parse_duration(duration_iso).total_seconds())

        # Fix Missing View Counts - Default to 0
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

# Function to Insert Videos into MySQL
def insert_videos(videos):
    conn = get_db_connection()
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
        conn.close()

# Test with a Sample Channel ID
if __name__ == "__main__":
    test_channel_id = os.getenv("TEST_CHANNEL_ID", "")
    if test_channel_id:
        videos = get_videos(test_channel_id)
        if videos:
            insert_videos(videos)
    else:
        print("No test channel ID provided.")

import mysql.connector
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime

# API Keys (Replace with environment variables or a secure method to store keys)
api_keys = [
    "YOUR_API_KEY_1",
    "YOUR_API_KEY_2",
    "YOUR_API_KEY_3",
    "YOUR_API_KEY_4"
]

# MySQL connection setup (Use environment variables for credentials)
def create_connection():
    return mysql.connector.connect(
        host="YOUR_HOST", 
        user="YOUR_USER",       
        password="YOUR_PASSWORD",  
        database="YOUR_DATABASE"   
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
    
    for api_key in api_keys:
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

    print(f"All API keys exceeded quota or failed for video {video_id}.")
    return None

# Function to insert comment data into MySQL
def insert_comment_data(connection, comment_data):
    try:
        cursor = connection.cursor()
        comment_data_list = [(comment['Comment_Id'], comment['Video_Id'], comment['Comment_Text'],
                              comment['Author'], comment['Published_Date'], comment['Likes'])
                             for comment in comment_data]

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

CHANNEL_ID = "YOUR_CHANNEL_ID"  # Replace with the desired channel ID
video_ids = get_video_ids(api_keys[0], CHANNEL_ID)
connection = create_connection()

for video_id in video_ids:
    print(f"Fetching comments for video {video_id}...")
    comments = get_video_comments(video_id)
    if comments:
        insert_comment_data(connection, comments)
    else:
        print(f"No comments found for video {video_id}.")

connection.close()

# Function to get playlists from a YouTube channel
def get_playlists(channel_id):
    playlists = []
    
    for api_key in api_keys:
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
                    playlists.append({
                        "playlist_id": item["id"],
                        "title": item["snippet"]["title"],
                        "channel_id": item["snippet"]["channelId"],
                        "channel_name": item["snippet"]["channelTitle"],
                        "published_at": convert_to_mysql_datetime(item["snippet"].get("publishedAt", None)),
                        "video_count": item["snippet"].get("itemCount", 0)
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

def insert_playlist_data(playlists, connection):
    cursor = connection.cursor()
    for playlist in playlists:
        try:
            cursor.execute("""
                INSERT INTO playlists (playlist_id, title, channel_id, channel_name, published_at, video_count)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (playlist["playlist_id"], playlist["title"], playlist["channel_id"], playlist["channel_name"], 
                  playlist["published_at"], playlist["video_count"]))
            connection.commit()
        except mysql.connector.Error as err:
            print(f"Error inserting playlist {playlist['title']}: {err}")
            connection.rollback()
    cursor.close()

for channel_id in [CHANNEL_ID]:
    playlists = get_playlists(channel_id)
    if playlists:
        insert_playlist_data(playlists, connection)

print("Playlist data inserted successfully!")

