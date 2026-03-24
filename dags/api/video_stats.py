import requests
import json
from datetime import date

#import os 
#from dotenv import load_dotenv
#load_dotenv(dotenv_path = "./.env")

from airflow.decorators import task
from airflow.models import Variable


API_KEY = Variable.get("API_KEY")
CHANNEL = Variable.get("CHANNEL_HANDLE")
max_results = 50

@task
def get_playlist_id():
    try:

        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL}&key={API_KEY}'

        response = requests.get(url)

        response.raise_for_status()

        data = response.json()

        channel_items = data["items"][0]
        channel_playlistID = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]

        #print(channel_playlistID)
        return channel_playlistID
    
    except requests.exceptions.RequestException as e:
        raise e

@task
def get_videos_ids(playlist_id):

    videos_ids = []
    page_token = None

    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={API_KEY}"

    try: 
        while True:
            url = base_url
            if page_token:
                url += f"&pageToken={page_token}"
            
            response = requests.get(url)

            response.raise_for_status()

            data = response.json()

            for item in data.get("items", []):
                videos_id = item["contentDetails"]["videoId"]
                videos_ids.append(videos_id)

            page_token = data.get('nextPageToken')
            
            if not page_token:
                break
        return videos_ids
    except requests.exceptions.RequestException as e:
        raise e

@task
def extract_video_data(video_ids):
    extracted_data = []

    def batch_list(video_id_list, batch_size):
        for video_id in range(0, len(video_id_list), batch_size):
            yield video_id_list[video_id : video_id + batch_size]
    
    try:
        for batch in batch_list(video_ids, max_results):
            video_ids_str = ",".join(batch)

            url = f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}'

            response = requests.get(url)

            response.raise_for_status()

            data = response.json()

            for item in data.get('items',[]):
                video_id = item['id']
                snippet = item['snippet']
                content_details = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                        "video_id" : video_id,
                        "title" : snippet['title'],
                        "published_at" : snippet['publishedAt'],
                        "duration" : content_details['duration'],
                        "view_count" : statistics.get('viewCount', None),
                        "like_count" : statistics.get('likeCount', None),
                        "comment_count" : statistics.get('commentCount', None)
                }

                extracted_data.append(video_data)
        
        return extracted_data
            
    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(extracted_data):
    file_path = f"./data/yt_data_{date.today()}.json"

    with open(file_path, "w", encoding="utf-8") as json_output:
        json.dump(extracted_data, json_output, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_videos_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)