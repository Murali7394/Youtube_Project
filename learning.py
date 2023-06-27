from googleapiclient.discovery import build
import pandas as pd
import seaborn as sns
import plotly.express as px
from pymongo import MongoClient
import sqlalchemy
from sqlalchemy import text
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns




def get_complete_data(channel_id):
    # api_key = "AIzaSyDm-I27KGIeEvGkhVYpvASmtjsh0kz9je8"
    api_key = 'AIzaSyDQO9hX_e0jV3e-QzX-c-AlwZMOZ_oJEYE'
    youtube = build('youtube', 'v3', developerKey=api_key)

    channel_data = get_all_of_channel(youtube, channel_id)
    playlist_id = channel_data.get('playlist_id')
    playlist_data = get_all_playlist(youtube, channel_id)
    videoID = get_all_videoIds(youtube, playlist_id)
    videos = get_video_details(youtube, videoID)

    complete_data = dict(channel=channel_data,
                         playlists=playlist_data,
                         video=videos)
    return complete_data


def get_all_of_channel(youtube, channel_id):
    request = youtube.channels().list(part='snippet,contentDetails,statistics',
                                      id=channel_id,
                                      )
    response = request.execute()

    channel_data = dict(channel_name=response['items'][0]['snippet']['title'],
                        channel_id=response['items'][0]['id'],
                        description=response['items'][0]['snippet']['description'],
                        pub_date=response['items'][0]['snippet']['publishedAt'],
                        sub_count=response['items'][0]['statistics']['subscriberCount'],
                        view_count=response['items'][0]['statistics']['viewCount'],
                        video_count=response['items'][0]['statistics']['videoCount'],
                        playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                        )

    return channel_data


def get_all_playlist(youtube, channel_id):
    request = youtube.playlists().list(part='snippet, contentDetails',
                                       channelId=channel_id,
                                       maxResults=25)
    response = request.execute()
    playlist_data = []
    for item in response['items']:
        data = dict(id=item['id'],
                    channel_id=item['snippet']['channelId'],
                    playlist_name=item['snippet']['title'],
                    no_of_videos=item['contentDetails']['itemCount'])
        playlist_data.append(data)

    return playlist_data



def get_all_videoIds(youtube, playlist_id):
    #     playlist_id = channel_data.get('playlist_id')

    # from here we are getting the vedio id by using the playlistid obtained from the channel data
    videoIds_request = youtube.playlistItems().list(part='snippet,contentDetails',
                                                    playlistId=playlist_id,
                                                    maxResults=50)
    videoIds_response = videoIds_request.execute()

    #     print(len(videoIds_response['items']))

    video_ids = []

    for id in videoIds_response["items"]:
        v_id = id['contentDetails']['videoId']
        video_ids.append(v_id)

    npt = videoIds_response.get('nextPageToken')
    np = True
    while np:
        if npt is None:
            np = False
        else:
            videoIds_request2 = youtube.playlistItems().list(part='snippet,contentDetails',
                                                             playlistId=playlist_id,
                                                             pageToken=npt,
                                                             maxResults=50)
            videoIds_response2 = videoIds_request2.execute()
            for id in videoIds_response2['items']:
                v_id = id['contentDetails']['videoId']
                video_ids.append(v_id)
            npt = videoIds_response2.get("nextPageToken")
    return video_ids


def get_all_comments(youtube, id):
    #     v_id = 'uBx4XZr1MaY'
    request = youtube.commentThreads().list(part='id, snippet, replies',
                                            videoId=id,
                                            maxResults=100)
    response = request.execute()
    all_comments = []
    for comment in response['items']:
        data = dict(video_id=comment['snippet']['videoId'],
                    comment_Id=comment['snippet']['topLevelComment']['id'],
                    comment_text=comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    author=comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    published_at=comment['snippet']['topLevelComment']['snippet']['publishedAt'],

                    )

        all_comments.append(data)
    next_page_token = response.get("nextPageToken")
    npt = True
    while npt:
        if next_page_token is None:
            npt = False
        else:
            request = youtube.commentThreads().list(part='id,snippet,replies',
                                                    videoId=id,
                                                    pageToken=next_page_token,
                                                    maxResults=100)
            response = request.execute()

            for comment in response['items']:
                data = dict(video_id=comment['snippet']['videoId'],
                            comment_Id=comment['snippet']['topLevelComment']['id'],
                            comment_text=comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            author=comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            published_at=comment['snippet']['topLevelComment']['snippet']['publishedAt']
                            )
                all_comments.append(data)
            next_page_token = response.get("nextPageToken")
    return all_comments


def get_video_details(youtube, videoID):
    video_ids = videoID[0:30]
    vedio_details = []
    for id in video_ids:
        request = youtube.videos().list(part='snippet, statistics, contentDetails',
                                        id=id)
        response = request.execute()

        data = dict(video_id=id,
                    channel_id=response['items'][0]['snippet']['channelId'],
                    video_name=response['items'][0]['snippet']['title'],
                    pub_date=response['items'][0]['snippet']['publishedAt'],
                    views=response['items'][0]['statistics']['viewCount'],
                    likes=response['items'][0]['statistics'].get('likeCount','0'),
                    dislikes=response['items'][0]['statistics'].get('dislikeCount','0'),
                    comment=response['items'][0]['statistics'].get('commentCount','0'),
                    fav=response['items'][0]['statistics'].get('favoriteCount','0'),
                    duration=response['items'][0]['contentDetails']['duration'],
                    caption=response['items'][0]['contentDetails']['caption'],
                    comments=get_all_comments(youtube, id)
                    )
        vedio_details.append(data)

    return vedio_details

def data_into_mongodb(datas):
    client = MongoClient("mongodb://127.0.0.1:27017/")
    mydb = client['Youtube_Project']
    mycol = mydb['youtube_channels']
    mycol.insert_many(datas)

def getting_the_channel_name(data):
    print(data.get('channel.channel_name'))


def mongodb_to_sql(option):
    client = MongoClient("mongodb://127.0.0.1:27017/")
    mydb = client['Youtube_Project']
    mycol = mydb['youtube_channels']
    output = mycol.find_one({'channel.channel_name': option}, {})
    engine = sqlalchemy.create_engine('sqlite:///Youtube_Project.db')
    # channel data frame that needed to uploaded to sql table
    channel_df = pd.DataFrame(output['channel'], index=[1])
    cols = ['sub_count', "view_count", 'video_count']
    for col in cols:
        channel_df[col] = pd.to_numeric(channel_df[col])
    channel_df['pub_date'] = pd.to_datetime(channel_df['pub_date']).dt.date

    playlist_df = pd.DataFrame(output['playlists'])

#   video dataframe that wanted to be updated to sql
    video_df = pd.DataFrame(output['video'])
    # print(video_df.columns)
    cols = ['views', "likes", 'comment', 'dislikes',"fav"]
    for col in cols:
        video_df[col] = pd.to_numeric(video_df[col])

    video_df['pub_date'] = pd.to_datetime(video_df['pub_date']).dt.date
    video_df.drop(columns='comments', inplace=True)

    for i in range(len(output['video'])):
        comments_df = pd.DataFrame(output['video'][i]['comments'])
        comments_df['published_at'] = pd.to_datetime(comments_df['published_at']).dt.date
        # engine = sqlalchemy.create_engine('sqlite:///sample.db')

        comments_df.to_sql('comments_data', engine, if_exists='append', index=False)

    channel_df.to_sql('channel_data', con=engine, if_exists='append', index=False)
    video_df.to_sql('video_data', con=engine, if_exists='append', index=False)
    playlist_df.to_sql('playlist_data', con=engine, if_exists='append', index=False)


st.write("Enter the youtube_ID for which the details to be analysed")

idtext = st.text_input('Enter the youtube channel id', label_visibility='visible',
                            placeholder='youtube_channel_id')
datas = []
channel_names = []
if idtext:
    channel_ids = idtext.split(',')
    for id in channel_ids:
        data = get_complete_data(id)
        channel_name = data['channel']['channel_name']
        datas.append(data)
        channel_names.append(channel_name)

    if st.button('Move to data lake'):
        data_into_mongodb(datas)

    option = st.selectbox('Choose the channel name to migrate to Data Warehouse', channel_names)
    if option:
        if st.button(option):
            mongodb_to_sql(option)

st.write("No of videos from each channel")
engine = sqlalchemy.create_engine('sqlite:///Youtube_Project.db')

query = text('SELECT channel_name, video_count FROM channel_data ORDER BY video_count DESC')
channel_videos = pd.read_sql(query, con=engine.connect())
st.bar_chart(data=channel_videos, x='channel_name', y='video_count')

st.write('Channel having highest no of views')
query = text('SELECT channel_name, view_count FROM channel_data ORDER BY video_count DESC')
channel_videos = pd.read_sql(query, con=engine.connect())
st.bar_chart(data=channel_videos)

query = text("""SELECT channel_name,video_name,views,likes FROM channel_data 
INNER JOIN video_data ON channel_data.channel_id = video_data.channel_id;
""")
videos_channels = pd.read_sql(query, con=engine.connect())
st.write('Name of the videos and thier channel names as a DataFrame')
st.dataframe(data=videos_channels)

st.write("Top 10 videos based on the the views")
query = text("""SELECT channel_name,video_name,views FROM video_data INNER JOIN channel_data ON video_data.channel_id = channel_data.channel_id
ORDER BY views DESC limit 10;""")
top_10 = pd.read_sql(query, con=engine.connect())
st.bar_chart(data=top_10, x='video_name', y='views')

st.write("Top 10 videos based on the the likes")
query = text("""SELECT channel_name,video_name,likes FROM video_data INNER JOIN channel_data ON video_data.channel_id = channel_data.channel_id
ORDER BY likes DESC limit 10;""")
top_10 = pd.read_sql(query, con=engine.connect())
st.bar_chart(data=top_10, x='video_name', y='likes')

st.write("Videos that has most no of comments top 10")
query = text("""SELECT channel_name,video_name,comment FROM video_data INNER JOIN channel_data ON video_data.channel_id = channel_data.channel_id
ORDER BY comment DESC limit 10;""")
top_10 = pd.read_sql(query, con=engine.connect())
st.bar_chart(data=top_10, x='video_name', y='comment')


st.write('The channels that have posted videos in the year 2022')
query = text("""SELECT channel_name , video_name , video_data.pub_date FROM video_data inner JOIN channel_data ON video_data.channel_id = channel_data.channel_id WHERE 
strftime('%Y', video_data.pub_date) == '2022';""")
video_2022 = pd.read_sql(query, con= engine.connect())
st.dataframe(video_2022)


st.write("How many comments were made on each video")
query = text("""SELECT channel_name, video_name, comment FROM video_data INNER JOIN channel_data ON video_data.channel_id = channel_data.channel_id
ORDER BY comment DESC;""")
commentno_df = pd.read_sql(query, con=engine.connect())
st.dataframe(commentno_df)