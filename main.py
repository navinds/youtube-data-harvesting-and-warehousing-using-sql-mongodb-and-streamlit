#============================================ Importing Required Libraries =============================================================================================#

from googleapiclient.discovery import build
import sqlalchemy as sa
from sqlalchemy import create_engine, inspect
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

#===================================================== Credentials and Connections ===========================================================#

load_dotenv()
mysql_password = os.getenv("MYSQL_PASSWORD") #MySQLpassword
api_key = os.getenv("API_KEY") #Youtube_API_V3
api_service_name = "youtube"
api_version = "v3"
mongo_atlas_user_name = os.getenv("MONGO_ATLAS_USER_NAME") #Mongo_Atlas_User_name
mongo_atlas_password =  os.getenv("MONGO_ATLAS_PASSWORD")  #Mongo_Atlas_password

# Building YouTube API Service
youtube = build(api_service_name, api_version, developerKey=api_key)

# Making connection between python and mongodb by passing a connection string in to a variable
client = pymongo.MongoClient(f"mongodb+srv://{mongo_atlas_user_name}:{mongo_atlas_password}@cluster0.mkrsiyl.mongodb.net/?retryWrites=true&w=majority")
mydb = client["yt_project_db"] #Creating Database
yt_data_collection = mydb["yt_data_hub"] # Connecting to the MongoDB collection

# Making connection between python and MySql by passing a connection string in to a variable
engine = sa.create_engine( f"mysql+pymysql://root:{mysql_password}@127.0.0.1/yt_project_db")

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

#Function to get playlist ids
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
# Function to get video details
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

def channel_data_to_mongodb(channel_ids):
    # Obtaining channel details, playlist ID, video IDs, video details, and comment data
    channel_details = get_channel_details(channel_ids)
    upload_ids = get_playlist_id(channel_ids)
    video_ids = get_video_ids(channel_ids)
    video_details = get_video_details(video_ids)
    comment_data = get_comment_data(video_ids)

    
    mydb = client["yt_project_db"]
    # Connecting to the MongoDB collection
    yt_data_collection = mydb["yt_data_hub"]

    # Inserting the collected data into MongoDB
    yt_data_collection.insert_one({"channel_details": channel_details,
                                   "video_details":video_details,
                                   "comment_data":comment_data})
    
    return("Uploaded Successfully to MongoDB!")

#----------------------------------------------------------------------------------------------------------------------#
   
def sql_channel_details_table(): # Creating SQL table for channel_details and Inserting values

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS channel_details")) # We are droping if table already exist to avoid duplicate data

    inspector = inspect(engine)

    if not inspector.has_table('channel_details'): # It will check channel details are already exist or not
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

    # Data Extraction from MongoDB
    mydb = client["yt_project_db"] #DataBase Name
    yt_data_collection = mydb["yt_data_hub"] # Collection
    channel_details = []
    for i in yt_data_collection.find({},{"_id":0,"channel_details":1}):
        channel_details.append(i["channel_details"][0])

    df = pd.DataFrame(channel_details)

    try:
        df = pd.DataFrame(channel_details)  # Create DataFrame from data
        df.to_sql('channel_details', con=engine, if_exists='append', index=False) #Inserting Channel Details to sql
        print("'channel_details' inserted successfully!")
    except Exception as e:
        print("Error inserting channel data:", e)

#----------------------------------------------------------------------------------------------------------------------#

def sql_video_details_table(): # Creating SQL table for video_details and Inserting values

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS video_details")) # We are droping if table already exist to avoid duplicate data

    inspector = inspect(engine)

    if not inspector.has_table('video_details'): # It will check video details are already exist or not
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
    
    # Data Extraction from MongoDB
    mydb = client["yt_project_db"] #DataBase Name
    yt_data_collection = mydb["yt_data_hub"] # Collection
    video_details = []
    for i in yt_data_collection.find({},{"_id":0,"video_details":1}):
        for j in range(len(i["video_details"])):
            video_details.append(i["video_details"][j])
        
    df = pd.DataFrame(video_details)

    try:
        df = pd.DataFrame(video_details)  # Create DataFrame from data
        df.to_sql('video_details', con=engine, if_exists='append', index=False,) #Inserting Video Details to sql

        print("'video_details' inserted successfully!")
    except Exception as e:
        print("Error inserting video data:", e)
        
#----------------------------------------------------------------------------------------------------------------------#

def sql_comment_data_table():

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS comment_data")) # We are droping if table already exist to avoid duplicate data

    inspector = inspect(engine)

    if not inspector.has_table('comment_data'): # It will check comment data are already exist or not
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
    
    
    mydb = client["yt_project_db"] #DataBase Name
    yt_data_collection = mydb["yt_data_hub"] # Collection
    comment_data = []
    for i in yt_data_collection.find({},{"_id":0,"comment_data":1}):
        for j in range(len(i["comment_data"])):
            comment_data.append(i["comment_data"][j])
        
    df = pd.DataFrame(comment_data)

    try:
        df = pd.DataFrame(comment_data)  # Create DataFrame from data
        df.to_sql('comment_data', con=engine, if_exists='append', index=False,) #Inserting comment data to sql

        print("'comment_data' inserted successfully!")
    except Exception as e:
        print("Error inserting comment data:", e)

#----------------------------------------------------------------------------------------------------------------------#

# Combaining all the sql table creation and data inserting function in a single function
def sql_tables():
    sql_channel_details_table()
    sql_video_details_table()
    sql_comment_data_table()

    return " All Tables and Values Loaded Successfully to SQL Database"

#================================================  Streamlit Zone ==================================================================#

# Below function is used to display the data which loaded to Mongodb using a Pandas Data Frame 

def streamlit_channel_details(): 
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
    mydb = client["yt_project_db"]
    yt_data_collection = mydb["yt_data_hub"]
    comment_data = []
    for i in yt_data_collection.find({},{"_id":0,"comment_data":1}):
        for j in range(len(i["comment_data"])):
            comment_data.append(i["comment_data"][j])
        
    df = st.dataframe(comment_data)

    return df

#---------------------------------------Streamlit Log In Page & Main Page-------------------------------------------------#

# Creating a log in Page
def login():

    #styles
    st.markdown("""
    <style>
    .title {
        font-family: 'Arial Black', sans-serif;
        font-size: 32px;
        font-weight: bold;
        color: #fff;
        background: linear-gradient(45deg, #C0392B, #000000, #FACC2E);
        padding: 15px;
        border-radius: 5px;
        box-shadow: 0 2px 5px rgba(0, 0, 0, 0.3);
        text-align: center;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="title">Navin's YT Data App</div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <style>
    .container {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        height: 100vh;
        background-color: #f5f5f5;
    }

    .form-container {
        background-color: #fff;
        padding: 30px;
        border-radius: 10px;
        box-shadow: 0px 2px 5px rgba(0, 0, 0, 0.1);
    }

    .title {
        text-align: center;
        font-size: 30px;
        font-weight: bold;
        margin-bottom: 20px;
    }

    .input-field {
        margin-bottom: 15px;
    }

    .input-field label {
        display: block;
        font-weight: bold;
        margin-bottom: 5px;
    }

    .input-field input {
        width: 100%;
        padding: 10px;
        border: 1px solid #ccc;
        border-radius: 5px;
    }

    .button {
        background-color: #4CAF50;
        color: white;
        padding: 10px 20px;
        border: none;
        border-radius: 5px;
        cursor: pointer;
    }

    .error-message {
        color: red;
        text-align: center;
        margin-top: 10px;
    }
    </style>
    """, unsafe_allow_html=True)


    st.container()

    with st.form("login_form"):
        st.markdown("<h1 style='font-size: 20px;'>Log in to YT Data App </h1>", unsafe_allow_html=True)

        username = st.text_input("Username", key="username")
        password = st.text_input("Password", type="password", key="password")

        if st.form_submit_button("Login"):
            # Check credentials (replace with your authentication logic)
            if username == "navin" and password == "#ydh&w":
                st.session_state.logged_in = True
                st.success("Login successful!")
                st.experimental_rerun()  # Redirect to the main app
            else:
                st.error("Invalid credentials")

# Creating Log out 
def logout():
    st.markdown("<h1 style='font-size: 20px;'>Log Out </h1>", unsafe_allow_html=True)

    if st.button("Logout"):
        st.session_state.logged_in = False
        st.success("Logout successful!")
        st.experimental_rerun()
#.............................................. Streamlit Main Page ............................................................#
def streamlit_interface():
    st.set_page_config(page_title="Navin's YouTube Data App", page_icon="📊")

    # Button Clicking Effect
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
        <h1 style='text-align: center; color: #FF3131; font-size: 2.5em; margin-bottom: 0.2em;'> 
            YouTube Data Harvesting and Warehousing
        </h1>
        <p style='text-align: center; font-size: 1.2em;'>🚀Navin's Youtube Data App!</p>
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

        # Email with logo
        email_logo = "https://img.icons8.com/fluent/48/000000/email.png"  
        your_email = "https://mail.google.com/mail/?view=cm&source=mailto&to=navinofficial1@gmail.com"
        st.markdown(f"[![Email]({email_logo})]({your_email})")

    #................................................. Core Page ............................................................#
        
    st.title("Welcome to the Navin's YouTube Data App!") #Title

    st.markdown("<h1 style='color: Grey;'># Get Data</h1>", unsafe_allow_html=True) 

    # Text input for channel ID 
    st.subheader("Step 1: Enter the Youtube Channel ID ")
    st.text("Please provide the Channel ID for scraping data into MongoDB:")
    channel_ids = st.text_input("Example: UCttEB90eQV25-u_U-W2o8mQ", max_chars=24, key="channel_id", help="Enter channel id and collect & store data")

    if not channel_ids:
        st.subheader("If you don't know the channel id  :red[click below] ⬇")
        st.link_button("Find Channel ID", "https://www.tunepocket.com/youtube-channel-id-finder/")
        st.stop()

    elif len(channel_ids) < 24:
        st.warning("Please enter a valid Channel ID.")
        st.stop()

    else:
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

    #Data Transfer to sql
    st.subheader("Step 2: Transfer Data to SQL")

    st.markdown('<p style="color: red;">🔽 <strong>Click the button below to start the data transfer to SQL</strong></p>', unsafe_allow_html=True)


    if st.button("Transfer to SQL",on_click=lambda: st.balloons()):
        # Create a spinner to show loading animation
        with st.spinner("Transferring data to SQL..."):
            # Simulate SQL migration
            display = sql_tables()
        
        # Display a success message after the spinner
        st.success("Data transfer to SQL completed successfully!")
        st.success(display)

    st.markdown("<h1 style='color: Grey;'># Analyze Data</h1>", unsafe_allow_html=True)
    st.markdown("<h3>Select a Query To Analyze</h3>", unsafe_allow_html=True)

    #...............................................Questions & Sql Queries............................................................#
    
    #Dropdown for selecting Questions
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



     # Displaying the output for selected questions 

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
    
    logout()

if __name__ == "__main__":
    if not hasattr(st.session_state, 'logged_in') or not st.session_state.logged_in:
        # If not logged in, display the login page
        login()
    else:
        # If logged in, display the main interface and logout button
        streamlit_interface()
        
#======================================================== THE END  =======================================================================================#

