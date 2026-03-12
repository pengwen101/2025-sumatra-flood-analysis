import os
import requests
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import time

load_dotenv()

DEVELOPER_KEY = os.getenv("YT_API_KEY")
yt_url = "https://www.googleapis.com/youtube/v3/"

yt_videos_cache_path = Path('cache/yt_videos_non_shorts_banjir_sumatera.csv')
yt_comments_cache_path = Path('cache/yt_comments_non_shorts_banjir_sumatera.csv')

if not yt_videos_cache_path.parent.exists():
    yt_videos_cache_path.parent.mkdir(parents=True)
    
def is_short(video_id):
    """
    Checks if a video is a YouTube Short by testing its URL behavior.
    Returns: True (is Short), False (is Regular Video)
    """
    url = f"https://www.youtube.com/shorts/{video_id}"
    
    try:
        response = requests.head(url, allow_redirects=False, timeout=5)
        if response.status_code == 200:
            return True
        elif response.status_code == 303:
            return False
        else:
            return False
            
    except requests.exceptions.RequestException:
        return False
    

def get_popular_videos(query, max_results, next_page_token=None, excluded_video_ids=['c5p7_3Kbxkc']):
    """
    Searches for videos by keyword (sorted by viewCount) and fetches details.
    """
    search_url = yt_url + "search"
    videos_url = yt_url + "videos"
  
    params = {
        "key": DEVELOPER_KEY,
        "q": query,
        "part": "id",
        "maxResults": min(max_results, 50),
        "type": "video",
        "order": "viewCount"
    }
    if next_page_token:
        params['pageToken'] = next_page_token
    
    fetched_videos = []
    total_fetched = 0
    current_search_token = next_page_token

    while total_fetched < max_results:
        try:
            response = requests.get(search_url, params=params).json()
        except Exception as e:
            print(f"Search network error: {e}")
            break

        if 'error' in response:
            print("Search API Error:", response['error']['message'])
            return None
        
        if 'items' not in response:
            break
            
        data = response['items']
        batch_next_token = response.get('nextPageToken')
        
        for d in data:
            if d['id']['videoId'] not in excluded_video_ids:
                fetched_videos.append({
                    'id': d['id']['videoId'], 
                    'next_page_token': batch_next_token 
                })
            
        total_fetched += len(data)
        
        if batch_next_token:
            params['pageToken'] = batch_next_token
            current_search_token = batch_next_token
        else:
            break

    fetched_videos = fetched_videos[:max_results]
 
    def chunk(lst, size=50):
        for i in range(0, len(lst), size):
            yield lst[i:i + size]
        
    all_video_details = []
    
    for batch in chunk(fetched_videos):
        video_ids_str = ",".join(v['id'] for v in batch)
        
        stats_params = {
            "key": DEVELOPER_KEY, 
            "part": "snippet,statistics", 
            "id": video_ids_str
        }
        
        try:
            stat_response = requests.get(videos_url, params=stats_params).json()
        except Exception as e:
            print(f"Video details network error: {e}")
            continue

        if 'items' in stat_response:
            for item in stat_response['items']:
                if is_short(item['id']):
                    continue
                
                stats = item.get('statistics', {})
                snippet = item.get('snippet', {})
                
                video_data = {
                    "keyword": query,
                    "id": item['id'],
                    "title": snippet.get('title'),
                    "description": snippet.get('description'),
                    "channel_title": snippet.get('channelTitle'),
                    "published_at": snippet.get('publishedAt'),
                    "view_count": int(stats.get('viewCount', 0)),
                    "like_count": int(stats.get('likeCount', 0)),
                    "comment_count": int(stats.get('commentCount', 0)),
                    "favorite_count": int(stats.get('favoriteCount', 0))
                }
                all_video_details.append(video_data)
    
 
    video_lookup = {v['id']: v['next_page_token'] for v in fetched_videos}
    for v in all_video_details:
        v['next_page_token'] = video_lookup.get(v['id'])
        
    return all_video_details

def get_video_comments(video_id, max_comments=100, page_token=None):
    """
    Fetches comments for a SINGLE video ID.
    Returns: (list_of_comments, next_page_token)
    """
    comments_data = []
    url = yt_url + "commentThreads"
    
    params = {
        "key": DEVELOPER_KEY,
        "textFormat": "plainText",
        "part": "snippet",
        "videoId": video_id,
        "maxResults": 100
    }
    
    if page_token:
        params['pageToken'] = page_token
        
    try:
        response = requests.get(url, params=params).json()
    except Exception as e:
        print(f"Comment network error for {video_id}: {e}")
        return [], None
        
    if 'error' in response:
        if "disabled" in response['error']['message']:
            return [], None
        print(f"API Error for {video_id}: {response['error']['message']}")
        return [], None
    
    if 'items' in response:
        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            comments_data.append({
                "video_id": video_id,
                "comment_id": item['id'],
                "author_name": comment.get('authorDisplayName'),
                "content": comment.get('textDisplay'),
                "published_at": comment.get('publishedAt'),
                "like_count": comment.get('likeCount'),
                "updated_at": comment.get('updatedAt'),
                "reply_count": item['snippet'].get('totalReplyCount', 0) 
            })
            
    next_token = response.get('nextPageToken')
    return comments_data, next_token

def get_and_store_all(keyword, num_videos=10, max_comment_counts=None, excluded_video_ids=['c5p7_3Kbxkc']):
    if yt_videos_cache_path.exists():
        yt_videos_df = pd.read_csv(yt_videos_cache_path)
    else:
        yt_videos_df = pd.DataFrame(columns=[
            "keyword", "id", "next_page_token", "title", "description", 
            "channel_title", "published_at", "like_count", "view_count", 
            "favorite_count", "comment_count"
        ])
    
    current_count = len(yt_videos_df[yt_videos_df['keyword'] == keyword])
    if current_count < num_videos:
        print(f"Fetching more videos for '{keyword}'...")
        resume_token = None
        if not yt_videos_df.empty:
            last_token = yt_videos_df.iloc[-1]['next_page_token']
            if pd.notna(last_token):
                resume_token = last_token
     
        needed = num_videos - current_count
        new_videos = get_popular_videos(keyword, max_results=needed, next_page_token=resume_token, excluded_video_ids=excluded_video_ids)
        
        if new_videos:
            print("Length of new videos", len(new_videos))
            new_df = pd.DataFrame(new_videos)
            existing_ids = set(yt_videos_df['id'])
            new_df = new_df[~new_df['id'].isin(existing_ids)]
            
            if not new_df.empty:
                yt_videos_df = pd.concat([yt_videos_df, new_df], ignore_index=True)
                yt_videos_df.to_csv(yt_videos_cache_path, index=False)
                print(f"Saved {len(new_df)} new videos.")
    else:
        print(f"Already have {current_count} videos for '{keyword}'.")

    if yt_comments_cache_path.exists():
        yt_comments_df = pd.read_csv(yt_comments_cache_path)
    else:
        yt_comments_df = pd.DataFrame(columns=[
            "video_id", "comment_id", "author_name", "content", 
            "published_at", "like_count", "updated_at", "reply_count", "next_page_token"
        ])

    target_video_ids = yt_videos_df[yt_videos_df['keyword'] == keyword]['id'].unique()
    
    print("Checking comments for videos...")
    
    for vid in target_video_ids:
        existing_comments = yt_comments_df[yt_comments_df['video_id'] == vid]
        count_existing = len(existing_comments)
        
        if max_comment_counts is not None:
            if count_existing >= max_comment_counts:
                continue
        
        print(f"Fetching comments for video {vid} (Have: {count_existing})...")
        
        if count_existing == 0:
            current_token = None
        else:
            current_token = existing_comments.iloc[-1]['next_page_token']
        fetched_count = 0
        
        if max_comment_counts is not None:
            condition = fetched_count < (max_comment_counts - count_existing)
        else:
            condition = True
        while condition:
            new_comments, next_token = get_video_comments(vid, page_token=current_token)
            if not new_comments:
                break
            new_comments_df = pd.DataFrame(new_comments)
            new_comments_df['next_page_token'] = next_token
            if not yt_comments_df.empty:
                is_new = ~new_comments_df['comment_id'].isin(yt_comments_df['comment_id'])
                new_comments_df = new_comments_df[is_new]
            
            if not new_comments_df.empty:
                yt_comments_df = pd.concat([yt_comments_df, new_comments_df], ignore_index=True)
                yt_comments_df.to_csv(yt_comments_cache_path, index=False)
                fetched_count += len(new_comments_df)
                print(f"  Saved {len(new_comments_df)} new comments.")
            
            if not next_token:
                break
            
            current_token = next_token
            time.sleep(0.5)

    print("Done processing.")

get_and_store_all('Banjir Sumatera', num_videos=26, max_comment_counts=None, excluded_video_ids=['c5p7_3Kbxkc'])