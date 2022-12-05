import traceback
import pandas as pd
import dill

import os
from dotenv import load_dotenv
from apiclient.discovery import build
from sqlalchemy import create_engine,text

load_dotenv()
key=os.getenv("GOOGLE_API_KEY")
youtube = build("youtube", "v3", developerKey=key)
#URL_DB = os.getenv("DATABASE_URL")
URL_DB = 'sqlite:///capstone.db'
db_engine = create_engine(URL_DB)
conn=db_engine.connect()


# def create_connections():
#     load_dotenv()
#     key=os.getenv("GOOGLE_API_KEY")
#     youtube = build("youtube", "v3", developerKey=key)
#     #URL_DB = os.getenv("DATABASE_URL")
#     URL_DB = 'sqlite:///capstone.db'
#     db_engine = create_engine(URL_DB)
#     conn=db_engine.connect()
#     return youtube, conn


def vid_by_kw(keyword):
    """Searches for Youtube videos related to keyword. 
    Each page contains at most 50 results. """
    npt=retrieve_NPT('vid_by_kw',keyword.replace(' ', '_'))
    try:
        result = youtube.search(
        ).list(q = keyword,
               pageToken=npt,
               part="snippet", 
               maxResults=50,
               order="relevance", 
               regionCode="US", 
               relevanceLanguage="en",
               type="video"
        ).execute()
        npt,vids=db_vid_by_kw(result,keyword)
        cache_NPT('vid_by_kw',keyword.replace(' ', '_'),npt)
        print(f"Keyword: {keyword}, Results: {len(result['items'])} videos, nextPageToken: {npt}")
        return vids
    except Exception as e:
        print("The error raised is: ", e)
        traceback_output = traceback.format_exc()
        print(traceback_output)        
        return None
        
def db_vid_by_kw(result,keyword):
    
    data=pd.DataFrame()
    data['video_url'] = [vid['id']['videoId'] for vid in result['items']]
    data['title'] = [vid['snippet']['title'] for vid in result['items']]
    data['description']  = [vid['snippet']['description'] for vid in result['items']]
    data['publishedAt']  = [vid['snippet']['publishedAt'] for vid in result['items']]
        
    response = youtube.videos(
    ).list(part="statistics",
           id=','.join(data['video_url'])
    ).execute()
    
    data['num_views'] = [vid_url['statistics'].get('viewCount',0) for vid_url in response['items']]  
    data['num_likes'] = [vid_url['statistics'].get('likeCount',0) for vid_url in response['items']]
    data['num_comments'] = [vid_url['statistics'].get('commentCount',0) for vid_url in response['items']]

    data.to_sql('youtube_videos',con=conn,if_exists='append',index=False)
        
    return result.get('nextPageToken',None), list(zip(data['video_url'], data['num_comments']))

def com_by_vid(video_url, num_comments):
    """Searches for comments on a given video url. 
    Each page contains at most 100 results. """
    npt=retrieve_NPT('com_by_vid',video_url)    
    num=0
    while num<num_comments:
        try:
            result=youtube.commentThreads(
            ).list(videoId=video_url,
                   pageToken=npt,
                   part='snippet', 
                   maxResults=100,
                   textFormat='plainText', 
                   order='time',        
            ).execute()
            npt=db_com_by_vid(result,video_url)     
            num+=len(result['items'])
            print(f"Video: {video_url}, Results: {num} of {num_comments} comments, nextPageToken: {npt}")
        except Exception as e:
            print("The error raised is: ",e)
            traceback_output = traceback.format_exc()
            print(traceback_output)   
            if num>0:
                cache_NPT('com_by_vid',video_url,npt)
            break

def db_com_by_vid(result,video_url):
    
    data=pd.DataFrame()
    data['comment_id']=[comment['snippet']['topLevelComment']['id'] for comment in result['items']]
    data['comments']=[comment['snippet']['topLevelComment']['snippet']['textOriginal'] for comment in result['items']]
    data['published_date']=[comment['snippet']['topLevelComment']['snippet']['publishedAt'] for comment in result['items']]
    data['like_count']=[comment['snippet']['topLevelComment']['snippet']['likeCount'] for comment in result['items']]
    data['viewer_rating']=[comment['snippet']['topLevelComment']['snippet']['viewerRating'] for comment in result['items']]
    data['video_url'] = [video_url for comment in result['items']]
    data.to_sql('youtube_comments',con=conn,if_exists='append',index=False)
    
    return result.get('nextPageToken',None)


def cache_NPT(api_call,identifier, npt):
    with open(api_call+'_'+identifier+'.pkd','wb') as cache:
        dill.dump(npt,cache)
        
def retrieve_NPT(api_call,identifier):
    try:
        with open(api_call+'_'+identifier+'.pkd','rb') as cache:
            npt=dill.load(cache)
    except:
        npt=None 
    return npt

            
if __name__=='__main__':
    import sys
    keyword=sys.argv[1]
    
    #youtube,conn=create_connections()
    while True:
        try:
            print("Searching for videos related to:", keyword,"...") 
            vids=vid_by_kw(keyword)
            for i in vids:
                print(f"Retrieving comments on video {i[0]} which has {i[1]} comments" )
                com_by_vid(i[0],int(i[1]))
        except Exception as e:
            print('The error raised is:', e)
            traceback_output = traceback.format_exc()
            print(traceback_output)   
            break
    
    
    