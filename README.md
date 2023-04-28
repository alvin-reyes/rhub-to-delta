# Radiant MLHub Data to Delta

A simple script to download datasets from Radiant MLHub and uploads all of them to Delta.

Note: This will run multiple threads on the host server. 

## Flow
- Pull all datasets from Radiant
- Download and save it on the host PC / server
- Upload all extracted and decompressed content to edge > delta

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
```bash
python main.py <miner> <api_key> <tag>
```

This generates a sqlite table with the following columns
- `name`
- `file_name`
- `cid`
- `size`
- `cid_url`
- `status`
- `created_at`
- `updated_at`

# Author
Outercore Engineering Team.
# rhub-to-delta
