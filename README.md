# Youtube_Harvesting_Warehouse
A Streamlit-based YouTube Data Analysis Dashboard that connects to MySQL to fetch and display YouTube channel, playlist, video, and comment data. The project includes a data harvesting script using the YouTube API to collect and store data in MySQL for in-depth insights.


# 📊 YouTube Data Harvesting & Warehousing with Streamlit and MySQL

This project is a **Streamlit-based web application** that allows users to **fetch, store, analyze, and visualize YouTube channel data**. It integrates **YouTube API, MySQL, and Streamlit** to create a **data warehousing system** with **SQL-based insights**.

## 🚀 Features

✅ **Fetch YouTube Data:** Retrieve channel details, playlists, videos, and comments.  
✅ **Store Data in MySQL:** Save and manage YouTube data in a structured format.  
✅ **Migrate Data to an Archive Table:** Backup video data for long-term storage.  
✅ **Streamlit Dashboard:** Interactive UI for data visualization and querying.  
✅ **SQL Insights:** Run predefined queries to analyze video performance, engagement, and trends.  

## 📌 Technologies Used

- **Python** (Data fetching, processing)
- **Streamlit** (Frontend UI)
- **MySQL** (Database storage)
- **YouTube API v3** (Data extraction)
- **Pandas** (Data manipulation)

## 📂 Project Structure

📁 youtube-data-warehouse │-- 📄 app.py # Streamlit dashboard │-- 📄 youtube_api.py # Fetching data from YouTube API -- 📄 
    requirements.txt # Python dependencies │-- 📄 README.md # Documentation


## 🔧 Installation

### 1️⃣ Clone the repository  
```sh
git clone https://github.com/your-repo/youtube-data-warehouse.git
cd youtube-data-warehouse

python -m venv venv
source venv/bin/activate   # On Windows: venv\Scripts\activate
pip install -r requirements.txt


API_KEY=your_youtube_api_key
DB_HOST=your_db_host
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_NAME=your_db_name

streamlit run app.py


📊 SQL Queries for Insights

1.) Videos and their Channels

2.) Channels with Most Videos

3.)Top 10 Most Viewed Videos

4.)Total Views per Channel

5.)Channels that Published Videos in 2022

6.)Average Video Duration per Channel

7.)Top 10 Videos with Most Comments

8.) Videos with Most Likes

9.) Total Likes per Video

10.) Videos with Most Liked Comments



Comments
🛠 Troubleshooting
⚠️ API quota exceeded? Try another API key.
⚠️ Database connection error? Check MySQL credentials in .env.
⚠️ No data found? Ensure the correct YouTube channel ID is entered.


🎯 Future Enhancements
🚀 Data visualizations with Matplotlib & Plotly
🚀 More advanced analytics using NLP for comments
🚀 Automated scheduling for data updates


Kadharibrahim0@gmail.com
This file provides clear instructions on setup, features, and usage. Let me know if you need any modifications! 🚀

