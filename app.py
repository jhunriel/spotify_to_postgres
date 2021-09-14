import datetime 
import requests
import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
load_dotenv()

dir_path = os.path.dirname(os.path.realpath(__file__))

def recently_played(data):
    song_name = []
    artist = []
    album = []
    played_at = []
    date = []
    for song in data["items"]:
        song_name.append(song["track"]["name"])
        artist.append(song["track"]["artists"][0]["name"])
        album.append(song["track"]["album"]["name"])
        played_at.append(song["played_at"])
        date.append(song["played_at"][0:10])

    recently_plated_dict = {
        "song_name":song_name,
        "artist":artist,
        "album":album,
        "played_at":played_at,
        "date":date
    }

    recently_plated_df = pd.DataFrame(recently_plated_dict)
    return recently_plated_df

def load_sql(df,file,sqltable):
    df.to_csv(file, header=True, index = False)
    conn = psycopg2.connect(
        host = os.getenv("HOST"),
        dbname = os.getenv("DBNAME"),
        user = os.getenv("USERNAME"),
        password = os.getenv("PASSWORD"),
        port = os.getenv("PORT")
    )
    cur = conn.cursor()
    sql = f'''
            COPY {sqltable}
            FROM '{os.path.join(dir_path,file)}'
            DELIMITER ',' CSV;
            '''
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    TOKEN = os.getenv("TOKEN")
    headers = {
            "Accept" : "application/json",
            "Content-Type" : "application/json",
            "Authorization" : "Bearer {token}".format(token=TOKEN)
        }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=12)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours      
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)
    data = r.json()
    recently_played_df = recently_played(data)
    print(recently_played_df)
    file = "spotify.csv"
    sqltable = "spotify"
    load_sql(recently_played_df,file,sqltable)




