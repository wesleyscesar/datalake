import requests
import json
from datetime import datetime
import pandas as pd
import itertools

current_date = datetime.now()

extract_date = current_date.strftime("%Y-%m-%d")

with open("/home/wesley/datalake/credentials.json", 'r') as arquivo:
    read_credentials = json.load(arquivo)
    token = read_credentials.get("token")

def create_folder(path):

    bronze_output_directory = f"datalake/bronze/{path}/{path}_{extract_date}.json"
    
    bronze_output_directory_str = str(bronze_output_directory)
    
    return bronze_output_directory_str


def create_json(records, file_path):
    with open(file_path, "w") as output_file:
        json.dump(records, output_file, ensure_ascii=False)
        output_file.write("\n")


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)    


def read_json(path):
    
    df = pd.read_json(f"/home/wesley/datalake/datalake/bronze/{path}/{path}_{extract_date}.json")
    
    if path == "videos":
        ids = [item.get("id").get("videoId") for sublist in df["items"] for item in sublist]
    else:
        ids = [item.get("id") for sublist in df["items"] for item in sublist]

    ids = [id_.replace(" ", "") for id_ in ids]

    return grouper(ids, 50)


def make_request(
        method,
        endpoint: str,
        params: dict[str, str] = None,
        headers: dict[str, str] = None,
) -> requests.Response:
    params = params or {}
    headers = headers or {}
    base_url = "https://www.googleapis.com/youtube/v3"
    url = f"{base_url}/{endpoint}"

    result = []

    response = requests.request(method, url, headers=headers, params=params)

    data = response.json()

    result.append(data)

    next_token = data.get("nextPageToken", "")

    while next_token:

        params["pageToken"] = next_token

        response = requests.request(method, url, headers=headers, params=params)

        data = response.json()

        result.append(data)

        next_token = data.get("nextPageToken", "")


    return result


def extract_channels():

    response_channels = make_request(
        method="GET",
        endpoint="channels",
        params = {
            "part": "snippet,statistics",
            "key": "AIzaSyC5UT680RNQe_XZn045ZQ7NczOrb-N2atk",
            "id": "UCa7sY4ir6zqXcI5sEs9Jhrw"
        }
    )

    create_json(records=response_channels, file_path=create_folder("channels"))


def extract_playlists():

    response_playlists = make_request(
        method="GET",
        endpoint="playlists",
        params = {
            "part": "snippet,contentDetails,id,player,status",
            "key": token,
            "channelId": "UCa7sY4ir6zqXcI5sEs9Jhrw",
            "maxResults": 50
        }
    )
    
    create_json(records=response_playlists, file_path=create_folder("playlists"))


def extract_playlist_items():

    id_groups = read_json(path="playlists")

    responses = []

    for id in id_groups:

        response_playlist_items = make_request(
            method="GET",
            endpoint = "playlistItems",
            params = {
                "key": token,
                "part": "id,snippet,status,contentDetails",
                "channelId": "UCa7sY4ir6zqXcI5sEs9Jhrw",
                "maxResults": 50,
                "playlistId": id
            }
        )

        responses.append(response_playlist_items)
    
    create_json(records=responses, file_path=create_folder("playlist_items"))


def extract_videos():

    response_videos = make_request(
        method="GET",
        endpoint = "search",
        params = {
            "key": token,
            "type": "video",
            "channelId": "UCa7sY4ir6zqXcI5sEs9Jhrw",
            "maxResults": 50
        }
    )
    
    create_json(records=response_videos, file_path=create_folder("videos"))


def extract_videos_details():

    id_groups = read_json(path="videos")

    responses = []

    for group in id_groups:

        valid_ids = [id_ for id_ in group if id_]
        
        ids_str = ','.join(valid_ids)
        
        response_videos_details = make_request(
            method="GET",
            endpoint="videos",
            params={
                "part": "id,snippet,statistics,contentDetails",
                "id": ids_str,
                "key": token,
                "channelId": "UCa7sY4ir6zqXcI5sEs9Jhrw",
                "maxResults": 50
            }
        )

        responses.append(response_videos_details)

    
    create_json(records=responses, file_path=create_folder("videos_details"))
    

extract_channels()
extract_playlists()
extract_videos()
extract_videos_details()
extract_playlist_items()