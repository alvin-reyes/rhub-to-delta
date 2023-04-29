# Radiant MLHub Data to Delta

A simple script to download datasets from Radiant MLHub and uploads all of them to Delta.

Note: This will run multiple threads on the host server. 

## Flow
- Pull all datasets from Radiant
- Download and save it on the host PC / server
- Upload all extracted and decompressed content to edge > delta
- Save all information to a local SQLlite DB for tracking

## Installation
```bash
pip install radiant_mlhub
```

## Usage
Initialize your Radiant API key
```
mlhub configure
API Key: Enter your API key here...
```

## Download the radiant earth dataset based on tags
View different tags here https://mlhub.earth/datasets?tags=xai
```bash
python main.py <miner> <api_key> <tags>
```

This generates a sqlite table with the following columns
- `name`
- `file_name`
- `edge_content_id`
- `cid`
- `size`
- `cid_url`
- `status`
- `created_at`
- `updated_at`

# Author
Outercore Engineering Team.
