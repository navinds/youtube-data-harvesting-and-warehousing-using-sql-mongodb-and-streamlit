#============================================= Importing Required Libraries =============================================================================================#

from googleapiclient.discovery import build
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy import text
import pymysql
import pymongo
import pandas as pd
import streamlit as st
import time
import logging
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import os


#================================================  Loading Credentials ===========================================================#

load_dotenv()
mysql_password = os.getenv("MYSQL_PASSWORD")
api_key = os.getenv("API_KEY")
api_service_name = "youtube"
api_version = "v3"
# Building YouTube API Service
youtube = build(api_service_name, api_version, developerKey=api_key)


#================================================= Data Scraping Zone  ========================================================================#

#This is a function to extract channel details from youtube server
def get_channel_details(channel_ids):

    all_channel_data =[] #channel datas will be appended here

    request = youtube.channels().list(
        part ="snippet,contentDetails,statistics", # Specify the parts of the channel resource to be included in the API response
        id = channel_ids)  # Specify the list of channel IDs for which details are requested  
    response = request.execute() # Scraped Data will be stored in this "response variable"

# we are using for loop for extract each and every data inside "items" in response.
    for i in response["items"]:
        # here we creating a dict by filtering the specific data that we need for this project
        channel_data = dict(
                    Channel_ID = i["id"], 
                    Channel_Name = i["snippet"]["title"],
                    Channel_Description = i['snippet']['description'],
                    Subscribers = i["statistics"]["subscriberCount"], #Subscriber_Count
                    Channel_Views =i["statistics"]["viewCount"],#Total_Channel_view
                    Video_Count = i["statistics"]["videoCount"], #Total_Posted_Video
                    Playlist_ID = i["contentDetails"]["relatedPlaylists"]["uploads"] #Video_Uploaded_ID
                    ) 
        all_channel_data.append(channel_data)
        
    return all_channel_data 

#----------------------------------------------------------------------------------------------------------------------#

#Function to get channel Ids
def get_playlist_id(channel_ids):
    for i in get_channel_details(channel_ids) : #reusing our previous "get_channel_data" function 
        if i["Channel_ID"] == channel_ids:
            return i["Playlist_ID"] #It will return the playlist_id of the given channel
    return None  # Return None if the channel name is not found


#----------------------------------------------------------------------------------------------------------------------#

#Function to get video ids
def get_video_ids(channel_ids):

    playlist_id = get_playlist_id(channel_ids) # reusing our "get_playlist_Id" function

# Making a request to the YouTube API to get playlist items (video_ids) based on the obtained Playlist ID
    request = youtube.playlistItems().list(
        part ="contentDetails",  # Specify the part of the resource to be returned (contentDetails includes videoId)
        playlistId= playlist_id, # Specify the Playlist ID to retrieve items (videos) from
        maxResults = 50)    # Maximum number of items to be returned in the API response
    response = request.execute()

# Extracting video IDs from the initial response
    video_ids = []
    for i in response['items']:
        data = i['contentDetails']['videoId']
        video_ids.append(data)

    next_page_token = response.get("nextPageToken") # Get the token for the next page of results, if available
    more_pages = True  # Set a flag to indicate whether there are more pages of results
    
     # Handling pagination to retrieve more video IDs if available
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part ="contentDetails",
                playlistId= playlist_id,
                maxResults = 50,
                pageToken = next_page_token)  # Specify the token for the next page of results
            response = request.execute()

            # Extracting video IDs from subsequent pages
            for i in response['items']:
                data = i['contentDetails']['videoId']
                video_ids.append(data)

            next_page_token = response.get("nextPageToken") # Update the next page token

            
    return video_ids

#----------------------------------------------------------------------------------------------------------------------#

def get_video_details(video_ids):

    all_video_details = [] # List to store details for each provided video ID

    for i in range(0,len(video_ids),50): 
        # Making a request to the YouTube API to get details for the specified video IDs
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_ids[i:i+50])) # Concatenate video IDs separated by commas
        response = request.execute()

        # Extracting details from the response for each video ID
        for i in response["items"]:
            data = dict( Channel_Name = i['snippet']['channelTitle'],
                         Channel_Id = i['snippet']['channelId'],
                         Video_Id =  i["id"],
                         Video_Title = i["snippet"]["title"],
                         Publish_Date = i["snippet"]["publishedAt"],
                         Video_Description = i["snippet"]["description"],
                         View_Count =  i["statistics"]["viewCount"],
                         Like_Count = i["statistics"]["likeCount"],
                         Favorite_Count = i["statistics"]["favoriteCount"],
                         Comment_Count = i["statistics"].get("commentCount"),
                         Duration = i["contentDetails"]["duration"],
                         Thumbnail = i['snippet']['thumbnails']['default']['url'],
                         Caption_Status = i['contentDetails']['caption'])


            all_video_details.append(data)
    return all_video_details
    
#----------------------------------------------------------------------------------------------------------------------#

#This is a function to get comment datas
def get_comment_data(video_ids):
    comment_data = []

    for i in video_ids:
        next_page_token = None
        comments_disabled = False

        while True:  # Continue fetching pages until there are no more comments
            try:
                request = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=i,
                    maxResults=100,
                    pageToken=next_page_token
                )
                response = request.execute()

                # Extract comments from the current page
                for item in response["items"]:
                    data = dict(
                        Channel_ID=item["snippet"]["channelId"],
                        Comment_ID=item["snippet"]["topLevelComment"]["id"],
                        Video_ID=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                        Comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                        Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        Comment_Published_Date=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                    )
                    comment_data.append(data)

                # Check for next page token
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break  # No more pages

            except HttpError as e:
                if e.resp.status == 403 and e.error_details[0]['reason'] == 'commentsDisabled':
                    comments_disabled = True
                    print(f"Comments are disabled for video ID: {i}")
                    break

                else:
                    raise

            if comments_disabled:
                break
    return comment_data


#=============================================   Data Storing Zone   ====================================================================#

# Making connection between python and mongodb by passing a connection string in to a variable
client = pymongo.MongoClient("mongodb://localhost:27017/")

def channel_data_to_mongodb(channel_ids):
    # Obtaining channel details, playlist ID, video IDs, video details, and comment data
    channel_details = get_channel_details(channel_ids)
    upload_ids = get_playlist_id(channel_ids)
    video_ids = get_video_ids(channel_ids)
    video_details = get_video_details(video_ids)
    comment_data = get_comment_data(video_ids)

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = client["yt_project_db"]

    # Connecting to the MongoDB collection
    yt_data_collection = mydb["yt_data_hub"]

    # Inserting the collected data into MongoDB
    yt_data_collection.insert_one({"channel_details": channel_details,
                                   "video_details":video_details,
                                   "comment_data":comment_data})
    
    return("Uploaded Successfully to MongoDB!")

#----------------------------------------------------------------------------------------------------------------------#
   
def sql_channel_details_table(mysql_password):
    engine = sa.create_engine( f"mysql+pymysql://root:{mysql_password}@127.0.0.1/yt_project_db")

    from sqlalchemy import create_engine, inspect

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS channel_details"))

    inspector = inspect(engine)

    if not inspector.has_table('channel_details'):
        with engine.connect() as conn:
            conn.execute(text("""
                            CREATE TABLE channel_details (
                            Channel_ID varchar(100) PRIMARY KEY,
                            Channel_Name varchar(100) ,
                            Channel_Description text,
                            Subscribers bigint ,
                            Channel_Views bigint ,
                            Video_Count int ,
                            Playlist_ID varchar(100))"""))
                            
            print("Table 'channel_details' created successfully!")
    else:
        print("Table 'channel_details' already exists.")

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = client["yt_project_db"]
    yt_data_collection = mydb["yt_data_hub"]
    channel_details = []
    for i in yt_data_collection.find({},{"_id":0,"channel_details":1}):
        channel_details.append(i["channel_details"][0])

    df = pd.DataFrame(channel_details)

    try:
        df = pd.DataFrame(channel_details)  # Create DataFrame from data
        df.to_sql('channel_details', con=engine, if_exists='append', index=False)
        print("'channel_details' inserted successfully!")
    except Exception as e:
        print("Error inserting channel data:", e)

#----------------------------------------------------------------------------------------------------------------------#

def sql_video_details_table(mysql_password):
    engine = sa.create_engine( f"mysql+pymysql://root:{mysql_password}@127.0.0.1/yt_project_db")

    from sqlalchemy import create_engine, inspect

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS video_details"))

    inspector = inspect(engine)

    if not inspector.has_table('video_details'):
        with engine.connect() as conn:
            conn.execute(text("""
                            CREATE TABLE video_details (
                            Channel_Name varchar(80),
                            Channel_ID varchar(50),
                            Video_ID varchar(100) PRIMARY KEY,
                            Video_Title varchar(300) ,
                            Publish_Date varchar(30),
                            Video_Description text ,
                            View_Count bigint ,
                            Like_Count bigint,
                            Favorite_Count bigint,
                            Comment_Count int,
                            Duration varchar(20),
                            Thumbnail varchar(200),
                            Caption_Status varchar(200))"""))
                            
            print("Table 'video_details' created successfully!")
    else:
        print("Table 'video_details' already exists.")

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = client["yt_project_db"]
    yt_data_collection = mydb["yt_data_hub"]
    video_details = []
    for i in yt_data_collection.find({},{"_id":0,"video_details":1}):
        for j in range(len(i["video_details"])):
            video_details.append(i["video_details"][j])
        
    df = pd.DataFrame(video_details)

    try:
        df = pd.DataFrame(video_details)  # Create DataFrame from data
        df.to_sql('video_details', con=engine, if_exists='append', index=False,)

        print("'video_details' inserted successfully!")
    except Exception as e:
        print("Error inserting video data:", e)
        
#----------------------------------------------------------------------------------------------------------------------#

def sql_comment_data_table(mysql_password):
    engine = sa.create_engine( f"mysql+pymysql://root:{mysql_password}@127.0.0.1/yt_project_db")

    from sqlalchemy import create_engine, inspect

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS comment_data"))

    inspector = inspect(engine)

    if not inspector.has_table('comment_data'):
        with engine.connect() as conn:
            conn.execute(text("""
                            CREATE TABLE comment_data (
                            Channel_ID varchar(100),
                            Comment_ID varchar(100),
                            Video_ID varchar(100) ,
                            Comment_Text text ,
                            Comment_Author varchar(200),
                            Comment_Published_Date varchar(50) )"""))
                            
            print("Table 'comment_data' created successfully!")
    else:
        print("Table 'channel_details' already exists.")
    
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = client["yt_project_db"]
    yt_data_collection = mydb["yt_data_hub"]
    comment_data = []
    for i in yt_data_collection.find({},{"_id":0,"comment_data":1}):
        for j in range(len(i["comment_data"])):
            comment_data.append(i["comment_data"][j])
        
    df = pd.DataFrame(comment_data)

    try:
        df = pd.DataFrame(comment_data)  # Create DataFrame from data
        df.to_sql('comment_data', con=engine, if_exists='append', index=False,)

        print("'comment_data' inserted successfully!")
    except Exception as e:
        print("Error inserting comment data:", e)

#----------------------------------------------------------------------------------------------------------------------#

def sql_tables(mysql_password):
    sql_channel_details_table(mysql_password)
    sql_video_details_table(mysql_password)
    sql_comment_data_table(mysql_password)

    return " All Tables and Values Loaded Successfully to SQL Database"


#=============================================  Streamlit Zone ==================================================================#


def streamlit_channel_details():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = client["yt_project_db"]
    yt_data_collection = mydb["yt_data_hub"]
    channel_details = []
    for i in yt_data_collection.find({},{"_id":0,"channel_details":1}):
        channel_details.append(i["channel_details"])

    flat_channel_details = [channel[0] for channel in channel_details]
    df = st.dataframe(flat_channel_details)

    return df

#----------------------------------------------------------------------------------------------------------------------#

def streamlit_video_details():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = client["yt_project_db"]
    yt_data_collection = mydb["yt_data_hub"]
    video_details = []
    for i in yt_data_collection.find({},{"_id":0,"video_details":1}):
        for j in range(len(i["video_details"])):
            video_details.append(i["video_details"][j])
        
    df = st.dataframe(video_details)

    return df

#----------------------------------------------------------------------------------------------------------------------#

def streamlit_comment_data():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = client["yt_project_db"]
    yt_data_collection = mydb["yt_data_hub"]
    comment_data = []
    for i in yt_data_collection.find({},{"_id":0,"comment_data":1}):
        for j in range(len(i["comment_data"])):
            comment_data.append(i["comment_data"][j])
        
    df = st.dataframe(comment_data)

    return df

#----------------------------------------------------------------------------------------------------------------------#

def streamlit_interface():
    st.set_page_config(page_title="YouTube Data App", page_icon="📊")

    button_style = """
        background-color: #3498db;
        color: white;
        padding: 10px 20px;
        font-size: 1.2em;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    """


    #..................................................Sidebar styling............................................................#
    
    with st.sidebar:
        st.markdown("""
        <h1 style='text-align: center; color: #2c3e50; font-size: 2.5em; margin-bottom: 0.2em;'>
            YouTube Data Harvesting and Warehousing
        </h1>
        <p style='text-align: center; font-size: 1.2em;'>🚀Youtube Data App!</p>
        """, unsafe_allow_html=True)

        st.header("Technology Used :")
        st.markdown("""
            - **Implemented backend logic with Python.**
            - **Developed using MongoDB and SQL for efficient data storage.**
            - **Integrated YouTube API for seamless communication.**
            - **Utilized Streamlit for web application development.**
        """)
        # About section
        st.header("About Me")
        st.markdown("""
            Hi, I'm Navin, deeply passionate about the sea of data science and AI. 
            My goal is to become a skilled data scientist.

            Beyond the lines of code, my aim is to innovate and be a part of transformative technological evolution. 
            The world needs solutions that not only solve problems but redefine them. 
            I'm here to create change.
        """)

        # LinkedIn link with logo
        st.header("Connect with Me")
        linkedin_logo = "https://img.icons8.com/fluent/48/000000/linkedin.png"  
        linkedin_url = "https://www.linkedin.com/in/navinkumarsofficial/"  
        st.markdown(f"[![LinkedIn]({linkedin_logo})]({linkedin_url})")
        email_logo = "https://img.icons8.com/fluent/48/000000/email.png"  
        your_email = "https://mail.google.com/mail/?view=cm&source=mailto&to=navinofficial1@gmail.com"
        st.markdown(f"[![Email]({email_logo})]({your_email})")


    #.............................................. Main Page ............................................................#
        
    st.title("Welcome to the YouTube Data App!")

    st.markdown("<h1 style='color: Grey;'># Get Data</h1>", unsafe_allow_html=True)

    # Text input for channel ID 
    st.subheader("Step 1: Enter the Channel ID")
    st.text("Provide the YouTube Channel ID:")
    channel_ids = st.text_input("Example: UC_x5XG1OV2P6uZZ5FSM9Ttw", max_chars=24, key="channel_id", help="Enter channel id and collect & store data")

    if not channel_ids:
        st.warning("Please enter a valid Channel ID.")
        st.stop()
    st.success(f"You entered: {channel_ids}")

    if st.button("Scrape and Store Data", key="unique_button", help="Click to initiate data scraping and storage", on_click=lambda: st.balloons()):
        with st.spinner("Collecting and storing data..."):
            channel_ids_list = []

            # Connect to MongoDB and access the collection
            mydb = client["yt_project_db"]
            yt_data_collection = mydb["yt_data_hub"]

            # Fetch existing channel IDs from the collection
            for item in yt_data_collection.find({}, {"_id": 0, "channel_details": 1}):
                channel_ids_list.append(item["channel_details"][0]["Channel_ID"])

            # Check if the provided Channel ID already exists
            if channel_ids in channel_ids_list:
                st.warning(f"Channel details for {channel_ids} already exist.")
            else:
                # Call your function to collect and store data
                output = channel_data_to_mongodb(channel_ids)
                st.success(output)

    # Increase the text size for the label using st.markdown
    st.markdown("<h3>View Scraped Data</h3>", unsafe_allow_html=True)

    # Table selection using st.selectbox with bold options using st.markdown
    show_table = st.selectbox("Select the Table for View", ["Choose a Table", "Channels", "Videos", "Comments"], index=0, help="Select a table to view data")

    # Button to trigger the display based on the selected table
    if st.button("Show Table"):
        if show_table == "Channels":
            streamlit_channel_details()
        elif show_table == "Videos":
            streamlit_video_details()
        elif show_table == "Comments":
            streamlit_comment_data()


    st.subheader("Step 2: Transfer Data to SQL")

    st.markdown('<p style="color: orange;">🔽 <strong>Click the button below to start the data transfer to SQL</strong></p>', unsafe_allow_html=True)


    if st.button("Transfer to SQL",on_click=lambda: st.balloons()):
        # Create a spinner to show loading animation
        with st.spinner("Transferring data to SQL..."):
            # Simulate SQL migration
            display = sql_tables(mysql_password)
        
        # Display a success message after the spinner
        st.success("Data transfer to SQL completed successfully!")
        st.success(display)


    engine = sa.create_engine( f"mysql+pymysql://root:{mysql_password}@127.0.0.1/yt_project_db")

    st.markdown("<h1 style='color: Grey;'># Analyze Data</h1>", unsafe_allow_html=True)
    st.markdown("<h3>Select a Query To Analyze</h3>", unsafe_allow_html=True)


    #...............................................Sql Queries............................................................#

    question = st.selectbox(
        'Please Select Your Question',
        ("Select a Query","Videos and their channels: Showcase video titles along with their corresponding channels.",
        "Channels with most videos: Highlight channels with the highest video counts and the number of videos.",
        "Top 10 viewed videos: Present the top 10 most viewed videos and their respective channel names.",
        "Comments per video: Display comment count and corresponding video names.",
        "Top liked videos: Show highest likes with respective channel names.",
        "Likes: Display total likes for each video along with names.",
        "Channel views: Showcase total views per channel with corresponding names.",
        "2022 Publishers: List channels that published videos in 2022.",
        "Avg. video duration: Present average duration for each channel's videos with names.",
        "Most commented videos: Show videos with the highest comments and their channel names."),help="Select pre written query to analyze data")




    engine = sa.create_engine( f"mysql+pymysql://root:{mysql_password}@127.0.0.1/yt_project_db")


    if st.button("Analyze Data"):
        st.write(f"Analyzing data for question: {question}")
        if question == "Videos and their channels: Showcase video titles along with their corresponding channels.":
            query_1 = pd.read_sql_query("select Channel_Name,Video_Title from video_details order by channel_name;",engine)
            st.write(query_1)

        elif question == "Channels with most videos: Highlight channels with the highest video counts and the number of videos.":
            query_2 = pd.read_sql_query('''select channel_name,count(Video_ID) as video_count 
                                        FROM video_details group by channel_name order by video_count desc;''',engine)
            st.write(query_2)

        elif question == "Top 10 viewed videos: Present the top 10 most viewed videos and their respective channel names.":
            query_3 = pd.read_sql_query('''select * from (select channel_name, video_title,view_count, 
                                        rank() over(partition by channel_name order by view_count desc) as video_rank
                                        from video_details where view_count is not null) as ranking  
                                        where video_rank <= 10;''',engine)
            st.write(query_3)

        elif question == "Comments per video: Display comment count and corresponding video names.":
            query_4 = pd.read_sql_query('''select b.video_title,a.video_id, count(a.comment_id) as comment_count 
                                        from comment_data as a left join video_details as b on a.Video_Id = b.Video_Id 
                                        group by a.video_id order by count(a.comment_id) desc;''',engine)
            st.write(query_4)

        elif question == "Top liked videos: Show highest likes with respective channel names.":
            query_5 = pd.read_sql_query('''select a.channel_name, a.Video_Title, a.like_count from 
                                        (select channel_name, Video_Title,like_count,rank() 
                                        over(partition by channel_name order by like_count desc)as ranking 
                                        from video_details) as a where ranking = 1;''',engine)
            st.write(query_5)

        elif question == "Likes: Display total likes for each video along with names.":
            query_6 = pd.read_sql_query('''select video_title ,like_count from video_details;''',engine)
            st.write(query_6)

        elif question == "Channel views: Showcase total views per channel with corresponding names.":
            query_7 = pd.read_sql_query('''select Channel_Name , channel_views from channel_details;''',engine)
            st.write(query_7)

        elif question == "2022 Publishers: List channels that published videos in 2022.":
            query_8 = pd.read_sql_query('''select distinct channel_name from video_details 
                                        where extract(year from publish_date) = 2022;''',engine)
            st.write(query_8)

        elif question == "Avg. video duration: Present average duration for each channel's videos with names.":
            query_9 = pd.read_sql_query('''SELECT b.channel_name,
                                        AVG(CAST(SUBSTRING(a.duration, 3, CHAR_LENGTH(a.duration) - 1) AS DECIMAL(10,2))) AS average_duration_in_minutes
                                        FROM video_details AS a
                                        JOIN channel_details AS b ON a.channel_id = b.channel_id
                                        GROUP BY b.channel_name;''',engine)
            st.write(query_9)

        elif question == "Most commented videos: Show videos with the highest comments and their channel names.":
            query_10 = pd.read_sql_query('''SELECT b.channel_name,b.video_title, count(a.comment_text) as comment_count 
                                            from comment_data as a left join video_details as b 
                                            on a.video_id = b.video_id group by a.video_id,b.channel_name 
                                            order by count(a.comment_text) desc ;''',engine)
            st.write(query_10)


streamlit_interface()


#======================================================== THE END  =======================================================================================#







