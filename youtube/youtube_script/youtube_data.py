from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dateutil.parser import isoparse
from datetime import datetime, timezone
import pandas as pd
import time
import itertools

now = datetime.now().strftime("%Y-%m-%d")

path = '폴더 위치'

df = pd.read_csv(f'{path}/youtube_name_video_id.csv')

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

data = {
    "fancim_name":[],
    "channel_id":[],
    "channels_name":[],
    "channels_customUrl":[],
    "channels_subscriberCount":[],
    "channels_viewCount":[],
    "channels_videoCount":[],
    "video_id":[],
    "video_publishedAt":[],
    "video_viewCount":[],
    "video_likeCount":[],
    "video_commentCount":[],
    "comment_textDisplay":[],    
    "comment_authorDisplayName":[],
    "comment_likeCount":[],
    "comment_totalReplyCount":[],
    "get_date" : []
}

data_size = zip(df['fancim_name'], df['channel_id'], df['video_id'])
batch_size = (len(df)//6+1)
api_keys = ['API key 값들'
]

for api_index, api_key in enumerate(api_keys):
    start_index = api_index * batch_size
    end_index = start_index + batch_size
    api_data = itertools.islice(data_size, start_index, end_index)
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=api_key)
    for fancim_name, channel_id, video_id in api_data:
        try:
            # 채널 정보
            channels_response = youtube.channels().list(
                id=channel_id,
                part='snippet,statistics'
                ).execute()
            time.sleep(1)
            # 비디오 정보
            video_response = youtube.videos().list(
                id = video_id,
                part='id,snippet,statistics').execute()
            time.sleep(1)
            commentThreads_response = youtube.commentThreads().list(
                    part="snippet,replies,id",
                    maxResults=1,
                    moderationStatus="published",
                    order="relevance",
                    videoId=video_id).execute()
            time.sleep(1)

            channels_name = channels_response['items'][0]['snippet']['title']
            channels_customUrl = channels_response['items'][0]['snippet']['customUrl']
            channels_subscriberCount = channels_response['items'][0]['statistics']['subscriberCount']
            channels_viewCount = channels_response['items'][0]['statistics']['viewCount']
            channels_videoCount = channels_response['items'][0]['statistics']['videoCount']
            video_publishedAt = video_response['items'][0]['snippet']['publishedAt'][:10]
            video_viewCount = video_response['items'][0]['statistics']['viewCount']
            video_likeCount = video_response['items'][0]['statistics']['likeCount']
            video_commentCount = video_response['items'][0]['statistics']['commentCount']
            comment_textDisplay = commentThreads_response['items'][0]['snippet']['topLevelComment']['snippet']['textOriginal']
            comment_authorDisplayName = commentThreads_response['items'][0]['snippet']['topLevelComment']['snippet']['authorDisplayName']
            comment_likeCount = commentThreads_response['items'][0]['snippet']['topLevelComment']['snippet']['likeCount']
            comment_totalReplyCount = commentThreads_response['items'][0]['snippet']['totalReplyCount']
            
            data['fancim_name'].append(fancim_name)
            data['channel_id'].append(channel_id)
            data['channels_name'].append(channels_name)
            data['channels_customUrl'].append(channels_customUrl)
            data['channels_subscriberCount'].append(channels_subscriberCount)
            data['channels_viewCount'].append(channels_viewCount)
            data['channels_videoCount'].append(channels_videoCount)
            data['video_id'].append(video_id)
            data['video_publishedAt'].append(video_publishedAt)
            data['video_viewCount'].append(video_viewCount)
            data['video_likeCount'].append(video_likeCount)
            data['video_commentCount'].append(video_commentCount)
            data['comment_textDisplay'].append(comment_textDisplay)
            data['comment_authorDisplayName'].append(comment_authorDisplayName)
            data['comment_likeCount'].append(comment_likeCount)
            data['comment_totalReplyCount'].append(comment_totalReplyCount)
            data['get_date'].append(now)
            time.sleep(1)
        
        except:

            data['fancim_name'].append(fancim_name)
            data['channel_id'].append(channel_id)
            data['channels_name'].append(None)
            data['channels_customUrl'].append(None)
            data['channels_subscriberCount'].append(None)
            data['channels_viewCount'].append(None)
            data['channels_videoCount'].append(None)
            data['video_id'].append(video_id)
            data['video_publishedAt'].append(None)
            data['video_viewCount'].append(None)
            data['video_likeCount'].append(None)
            data['video_commentCount'].append(None)
            data['comment_textDisplay'].append(None)
            data['comment_authorDisplayName'].append(None)
            data['comment_likeCount'].append(None)
            data['comment_totalReplyCount'].append(None)
            data['get_date'].append(now)

        finally:
            df1 = pd.DataFrame(data)
            df1.to_csv(f'{path}/youtube_data.csv')

df1 = pd.DataFrame(data)
df1.to_csv(f'{path}/youtube_data.csv')

# print('channels name:', channels_response['items'][0]['snippet']['title'])
# print('channels customUrl:', channels_response['items'][0]['snippet']['customUrl'])
# print('channels publishedAt:', channels_response['items'][0]['snippet']['publishedAt'][:10])
# print('channels subscriberCount:', int(channels_response['items'][0]['statistics']['subscriberCount']))
# print('channels viewCount:', int(channels_response['items'][0]['statistics']['viewCount']))
# print('channels videoCount:', int(channels_response['items'][0]['statistics']['videoCount']))

# print('videoid:', video_response['items'][0]['id'])
# print('video publishedAt:', video_response['items'][0]['snippet']['publishedAt'])
# print('video viewCount:', video_response['items'][0]['statistics']['viewCount'])
# print('video likeCount:', video_response['items'][0]['statistics']['likeCount'])
# print('video commentCount:', video_response['items'][0]['statistics']['commentCount'])

# print('comment textDisplay:', commentThreads_response['items'][0]['snippet']['topLevelComment']['snippet']['textOriginal'])
# print('comment authorDisplayName:', commentThreads_response['items'][0]['snippet']['topLevelComment']['snippet']['authorDisplayName'])
# print('comment likeCount:', commentThreads_response['items'][0]['snippet']['topLevelComment']['snippet']['likeCount'])
# print('comment totalReplyCount:', commentThreads_response['items'][0]['snippet']['totalReplyCount'])

