from pymongo import MongoClient
import certifi
import requests
import sys
from time import sleep
import os
from dotenv import load_dotenv 
load_dotenv()

def get_database():
    password = os.getenv("DB_PASSWORD")
    user = os.getenv("DB_USER")
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = f"mongodb+srv://{user}:{password}@cluster0.gpzb5.mongodb.net/tailnode?retryWrites=true&w=majority"
    client = MongoClient(CONNECTION_STRING, tlsCAFile = certifi.where())
    return client['tailnode']
    
# This is added so that many files can reuse the function get_database()
def store_userData(collection_name):
    url = 'https://dummyapi.io/data/v1/user'
    headers = {
    'app-id': '6246c4dc735be552983eed59'
    }
    r = requests.get(url, headers=headers)
    file = r.json()
    data = file['data']
    print(f'Fetched records from {url}\ncount : {len(data)}')
    collection_name.insert_many(data)

def delete_collecion(collection_name):
    print(f'data cleanup in process...\n')
    collection_name.drop({})

def get_users(collection_name, dbname):
    cursor = collection_name.find({})
    mydb = dbname['user_posts']
    delete_collecion(mydb)
    list_cursor = list(cursor)
    max_bars_count = 20
    spaces = f"%-{max_bars_count}s"
    count = len(list_cursor)
    print("Uploading\n")
    i = 1
    for document in list_cursor:

        j = i / count
        sys.stdout.write('\r')
        sys.stdout.write(f"{spaces} %d%%" % ('#'*int(j*max_bars_count), 100*j))
        sys.stdout.flush()
        sleep(.25)

        user_id = document['id']
        posts = f'https://dummyapi.io/data/v1/user/{user_id}/post'
        headers = {
        'app-id': '6246c4dc735be552983eed59'
        }
        r = requests.get(posts, headers=headers)
        file = r.json()
        post_data = file['data']
        mydb.insert_many(post_data)
        i += 1
    j = i / count
    print("\n\nUploaded")

    



if __name__ == "__main__":    
    
    # Get the database
    dbname = get_database()
    collection_name = dbname['users']
    # delete old records
    delete_collecion(collection_name)
    # store new records from API
    store_userData(collection_name)
    get_users(collection_name, dbname)


