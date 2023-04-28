import http.client
import json
import mimetypes
import os
import sqlite3
import ssl
import datetime
import sys
from codecs import encode
from radiant_mlhub import Dataset
import threading
from apscheduler.schedulers.blocking import BlockingScheduler

context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE

# non ssl connection
conn = http.client.HTTPSConnection(
    host='edge.estuary.tech',
)

# function to upload to delta and accepts a file path
def upload_to_delta(file_path, miner, estuary_api_key):
    cursor = sqlconn.cursor()

    # if filename exists in database, skip
    cursor.execute("SELECT * FROM rhub_data WHERE file_path=?", (file_path,))
    if cursor.fetchone() is not None:
        return

    filename = file_path.split('/')[-1]
    dataList = []
    boundary = 'wL36Yn8afVp8Ag7AmP8qZ0SA4n1v9T'
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=data; filename={0}'.format(filename)))

    fileType = mimetypes.guess_type(file_path)[0] or 'application/octet-stream'
    dataList.append(encode('Content-Type: {}'.format(fileType)))
    dataList.append(encode(''))

    with open(file_path, 'rb') as f:
        dataList.append(f.read())
    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=metadata;'))

    dataList.append(encode('Content-Type: {}'.format('text/plain')))
    dataList.append(encode(''))
    dataList.append(encode('--' + boundary + '--'))
    dataList.append(encode(''))
    body = b'\r\n'.join(dataList)
    payload = body
    headers = {
        'Authorization': 'Bearer ' + estuary_api_key,
        'Content-type': 'multipart/form-data; boundary={}'.format(boundary)
    }
    conn.request("POST", "/api/v1/content/add", payload, headers)
    res = conn.getresponse()
    data = res.read()
    responseJson = json.loads(data)

    cid = responseJson["contents"][0]["cid"]
    name = responseJson["contents"][0]["name"]
    size = responseJson["contents"][0]["size"]
    ret_url = "https://edge.estuary.tech/gw/" + responseJson["contents"][0]["cid"]

    cursor.execute(
        "INSERT INTO rhub_data ("
        "name, file_path, cid, size, cid_url, status, api_key, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, "
        "?, ?)",
        (
            name,
            file_path,
            cid,
            size,
            ret_url,
            res.status,
            estuary_api_key,
            datetime.datetime.now(),
            datetime.datetime.now()
        )
    )
    sqlconn.commit()


## function to get all files in a directory
def get_all_files(directory):
    # initializing empty file paths list
    file_paths = []

    # crawling through directory and subdirectories
    for root, directories, files in os.walk(directory):
        for filename in files:
            # join the two strings in order to form the full filepath.
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)

    # returning all file paths
    return file_paths

## function to process the dataset
def process_data_set(dataset, location="./all_datasets/"):
    dataset.download(location)

## create the database if not exists
def create_sql_db():
    sqlconn = sqlite3.connect('rmhub_to_delta.db')
    cursor = sqlconn.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS rhub_data ("
                   "name TEXT, file_path TEXT, cid TEXT, size TEXT, cid_url TEXT,"
                   "status TEXT,api_key TEXT, created_at DATE, updated_at DATE)")

    return sqlconn


## main
miner = sys.argv[1]
estuary_api_key = sys.argv[2]
tags = sys.argv[3]
print("miner: " + miner)
print("estuary_api_key: " + estuary_api_key)
print("tags: " + tags)

# set up the database/table if not exists
sqlconn = create_sql_db()

# prepare the dataset
datasets = Dataset.list(tags=tags)

scheduler = BlockingScheduler()
for dataset in datasets:
    scheduler.add_job(process_data_set, 'interval', args=(dataset[0],), seconds=5)
    scheduler.start()

files = get_all_files("./all_datasets/")
for file in files:
    upload_to_delta(file, miner, estuary_api_key)
