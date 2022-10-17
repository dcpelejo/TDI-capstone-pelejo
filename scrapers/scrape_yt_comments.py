import pandas as pd
import dill

import os
from dotenv import load_dotenv
from apiclient.discovery import build
from sqlalchemy import create_engine,text


def create_connections():
    load_dotenv()
    key=os.getenv("GOOGLE_API_KEY")
    youtube = build("youtube", "v3", developerKey=key)
    URL_DB = os.getenv("DATABASE_URL")
    db_engine = create_engine(URL_DB)
    conn=db_engine.connect()
    return youtube, conn


def vid_by_kw(keyword):
    """Searches for Youtube videos related to keyword. 
    Each page contains at most 50 results. """
    npt=retrieve_NPT(keyword.replace(' ', '_'),'vid_by_kw')
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
    
    data['num_views'] = [keyword['statistics']['viewCount'] for vid_url in response['items']]  
    data['num_likes'] = [keyword['statistics']['likeCount'] for vid_url in response['items']]
    data['num_comments'] = [keyword['statistics']['commentCount'] for vid_url in response['items']]

    data.to_sql('youtube_videos',con=conn,if_exists='append',index=False)
        
    return result['nextPageToken'], list(zip(data['video_url'], data['num_comments']))

def com_by_vid(video_url, num_comments):
    """Searches for comments on a given video url. 
    Each page contains at most 100 results. """
    npt=retrieve_NPT(video_url,'com_by_vid')    
    num=0
    while num<num_comments:
        try:
            result=youtube.commentThreads(
            ).list(videoId='video_url',
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
    
    return result['nextPageToken']


def cache_NPT(identifier, api_call, npt):
    with open(api_call+identifier+'.pkd','wb') as cache:
        dill.dump(npt,cache)
        
def retrieve_NPT(identifier,api_call):
    try:
        with open(api_call+identifier+'.pkd','rb') as cache:
            npt=dill.load(cache)
    except:
        npt=None 
    return npt

            
if __name__=='__main__':
    import sys
    keyword=sys.argv[1]
    
    youtube,conn=create_connections()
    while True:
        try:
            vids=vid_by_kw(keyword)
            for i in vids:
                com_by_vid(i[0],int(i[1]))
        except Exception as e:
            print('The error raised is:', e)
            break
    
    
    