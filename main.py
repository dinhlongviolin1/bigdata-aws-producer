import json
import urllib3
import ast
import boto3
from datetime import datetime
import time
from config import *

http = urllib3.PoolManager()
kinesis_client = boto3.client('kinesis', region_name='us-east-1', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)

if __name__ == "__main__":
    print("inside main")
    symbol = "BTCUSDT"
    interval = "1m"
    while True:
        r = http.request('GET', f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit=1")
        data = r.data
        dict_str = data.decode("UTF-8")
        data = ast.literal_eval(dict_str)
        data = data[0]
        data_formatted = {
            "symbol": str(symbol),
            "interval": str(interval),
            "open_time": int(data[0]),
            "open": round(float(data[1]), 8),
            "high": round(float(data[2]), 8),
            "low": round(float(data[3]), 8),
            "close": round(float(data[4]), 8),
            "volume": float(data[5]),
            "close_time": int(data[6]),
            "trades_count": int(data[8])
        }
        data_output = json.dumps(data_formatted) + "\n"
        print("Sending data to Kinesis!")
        kinesis_client.put_record(Data=data_output,StreamName = "binance-stream-1m", PartitionKey=str(datetime.now()))
        print("Data sent to Kinesis successfully!")
        print(data_output)
        time.sleep(60)
