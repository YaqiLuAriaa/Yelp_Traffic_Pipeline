import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage

# Config
API_KEY = "1Na594Gy6aZnosQWybyUq62FucHyMl1S"
BUCKET_NAME = "traffic-data-lake-us"

# Coordinates (Philadelphia traffic points)
coords = [
"39.9485704,-75.1600722",
"39.94471054,-75.17496482",
"39.9499363,-75.1616923",
"39.9468321,-75.1764911",
"39.9518941,-75.1769506",
"39.968969,-75.13953142",
"39.9389221,-75.146352",
"39.950504,-75.169215",
"39.9497816,-75.1601221",
"39.9302488,-75.1630919",
"40.05342,-74.999245",
"39.951647,-75.155749",
"39.9484908,-75.1535576",
"40.02539306,-75.20828469",
"39.952053,-75.171493",
"39.9559288,-75.1574567",
"39.9682609,-75.1749671",
"39.96826,-75.17497",
"39.9771201,-75.18218994",
"39.978906,-75.130459",
"39.942203,-75.155223",
"39.9519105,-75.1443815",
"39.937636,-75.15808247",
"39.95093987,-75.14586752",
"39.965684,-75.135418"
]

ids = [
    'l_7TW_Ix58-QvhQgpJi_Xw',
    'Og4z8nB4ZMZs3oHkVhB_pA',
    'qETpOF88MbmtSyOWEDaD_A',
    '7zJ2m_b-VdThJ5zpnwXSOQ',
    'BZU9iLoVPeBzUlp1s8756A',
    'rQ2CSHRkZSn5WCu7OfVI-Q',
    '23H5J1Y5rRMLU8dMxN7EPA',
    '-cSHu3A9IalZn8s7NGJagw',
    'kjKImJ8dLwN4jDi5PQP46g',
    'ROs0p-56kxQL2lgV00c0yQ',
    'o_N7HiU3f6cVvrJAFrE43A',
    '1kRUUIg0EsHQndMfMC2x9g',
    'xXD8QNGjjIMgtTGwceriJQ',
    'KB3I1jQFeJqCHhVHY7TieQ',
    'cyl0ML6y0jj8Jo_sh3M6Cg',
    'u3zqvp4BYUjzJD7tzx3Jbg',
    'B_Kk8Nq9NqfvwFrN8uOR9w',
    'akNpPdFsHk0mvbqkmPrXWg',
    'l5nQUiJmfzg-SGR0YrXtdA',
    'i9n3DzCkmWDfiJ7yzTxPJg',
    'qjGS_7iaQDpbVhS6W8qkHQ',
    '-fs09akgCKv5rTTy7iUHUg',
    'TE2IEDNV0RcI6s1wTOP4fg',
    'mmBwAe9q062vKWanRfoagQ',
    'eAzeb04i354i6VZFI4bJRQ'
]

url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"


def poll_traffic(request):

    results = []

    for coord, business_id in zip(coords, ids):

        params = {
            "point": coord,
            "unit": "KMPH",
            "key": API_KEY
        }

        r = requests.get(url, params=params).json()

        record = r.get("flowSegmentData", {})
        record["coord"] = coord
        record["city"] = "Philadelphia"
        record["BUSINESS_ID"] = business_id
        record["timestamp"] = datetime.utcnow().isoformat()

        results.append(record)

    # Flatten JSON
    df = pd.json_normalize(results)

    # Create parquet
    timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"traffic_{timestamp_str}.parquet"
    local_path = f"/tmp/{filename}"

    df.to_parquet(local_path, index=False)

    # Upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    gcs_path = f"tomtom/year={datetime.utcnow().year}/month={datetime.utcnow().month}/day={datetime.utcnow().day}/{filename}"

    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)

    return f"Uploaded {filename} with {len(df)} rows to {gcs_path}"
