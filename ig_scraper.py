import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# Define list of hashtags and base URL

hashtags = ['banjirsumatra', 'banjirsumatera']
graph_url = 'https://graph.facebook.com/v15.0/'
hashtag_ids_cache_path = Path('cache/hashtag_ids.csv')
hashtag_medias_cache_path = Path('cache/hashtag_medias.csv')
media_comments_cache_path = Path("cache/media_comments.csv")
ACCOUNT_ID = os.getenv('ACCOUNT_ID')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')

# Get hashtag ID

def get_hashtag_id(hashtag_name, user_id, access_token):
    url = graph_url + "ig_hashtag_search"
    param = dict()
    param['user_id'] = user_id
    param['q'] = hashtag_name
    param['access_token'] = access_token
    response = requests.get(url, params=param)
    response = response.json()
    if 'error' in response:
        print("Error:", response['error']['message'])
        return
    if 'data' in response:
        data = response.get('data')
        id = data[0]['id']
        return id
    else:
        return None

# Get hashtag top medias

def get_hashtag_top_medias(hashtag_id, max_pages, user_id, access_token, after=None):
    url = graph_url + str(hashtag_id) + "/top_media"
        
    params = {
        'user_id': user_id,
        'fields': 'id,caption,media_type,comments_count,like_count,permalink,timestamp,media_url',
        'limit':50,
        'access_token': access_token
    }
    
    if after:
        params['after'] = after
    
    all_media = []
    page_count = 0
    
    while page_count < max_pages:
        response = requests.get(url, params=params).json()
        
        if 'error' in response:
            print("Error:", response['error']['message'])
            
        if 'data' in response:
            all_media.extend(response['data'])
            print(f"Fetched {len(response['data'])} posts from page {page_count + 1}")
        
        # Check if there is a next page
        if 'paging' in response and 'cursors' in response['paging'] and 'after' in response['paging']['cursors']:
            params['after'] = response['paging']['cursors']['after']
            page_count += 1
            time.sleep(1)
        else:
            print("No more pages available.")
            break
    
    return {
        "after": params.get('after', None),
        "medias": all_media
        }     

def get_and_store_all(hashtags, max_media_count=1000, top_k=10):
    if hasattr(hashtag_ids_cache_path, 'parent'):
        os.makedirs(hashtag_ids_cache_path.parent, exist_ok=True)
    
    if hasattr(hashtag_medias_cache_path, 'parent'):
        os.makedirs(hashtag_medias_cache_path.parent, exist_ok=True)
    
    if hashtag_ids_cache_path.exists():
        hashtag_id_df = pd.read_csv(hashtag_ids_cache_path, header=0)
    else:
        hashtag_id_df = pd.DataFrame({"hashtag": [], "id": []})
        
    hashtag_ids = hashtag_id_df.to_dict(orient="list")
    
    for hashtag in hashtags:
        id = None
        
        row = hashtag_id_df[hashtag_id_df['hashtag']==hashtag]
        if row.empty:
            # If not found, call API
            id = get_hashtag_id(hashtag, ACCOUNT_ID, ACCESS_TOKEN)
            if id is not None:
                hashtag_ids['hashtag'].append(hashtag)
                hashtag_ids['id'].append(id)
            
    hashtag_id_df = pd.DataFrame(hashtag_ids)
    
    # Save to cache
    hashtag_id_df.to_csv(hashtag_ids_cache_path, index=False)
    
    if hashtag_medias_cache_path.exists():
        hashtag_medias_df = pd.read_csv(hashtag_medias_cache_path, header=0)
    else:
        hashtag_medias_df = pd.DataFrame({"hashtag_id": [], "media_id": [],
                                          "caption":[],"media_type":[],"comments_count":[],
                                          "like_count":[],"permalink":[],"timestamp":[],
                                          "media_url":[], "after":[]})
        
    existing_ids = set(hashtag_medias_df['media_id'])
    hashtag_medias = hashtag_medias_df.to_dict(orient="list")
    
    for hashtag_id in hashtag_id_df['id']:
        rows = hashtag_medias_df[hashtag_medias_df['hashtag_id']==hashtag_id]
        print(f"Found {len(rows)} from cache for hashtag id {hashtag_id}")
        if rows.empty:
            results = get_hashtag_top_medias(hashtag_id, 10, ACCOUNT_ID, ACCESS_TOKEN)
            
        elif len(rows) < max_media_count:
            after = rows.iloc[-1]['after']
            results = get_hashtag_top_medias(hashtag_id, 10, ACCOUNT_ID, ACCESS_TOKEN, after)
        else:
            continue
        after = results['after']
        medias = results['medias']
        
        for media in medias:
            if media['id'] in existing_ids:
                continue 
            hashtag_medias['hashtag_id'].append(hashtag_id)
            hashtag_medias['media_id'].append(media.get('id'))
            hashtag_medias['caption'].append(media.get('caption'))
            hashtag_medias['media_type'].append(media.get('media_type'))
            hashtag_medias['comments_count'].append(media.get('comments_count'))
            hashtag_medias['like_count'].append(media.get('like_count'))
            hashtag_medias['permalink'].append(media.get('permalink'))
            hashtag_medias['timestamp'].append(media.get('timestamp'))
            hashtag_medias['media_url'].append(media.get('media_url'))
            hashtag_medias['after'].append(after)
            existing_ids.add(media['id'])
        
    hashtag_medias_df = pd.DataFrame(hashtag_medias)
    hashtag_medias_df.to_csv(hashtag_medias_cache_path, index=False)
    
    
get_and_store_all(hashtags, max_media_count=1000)