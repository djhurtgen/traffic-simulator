"""
Created on Thu Apr 13 2023

@author David Hurtgen

Kafka consumer for loading traffic data into AWS DynamoDB and S3
Objects are json files with 1 set of values for each road segment
(One complete run of the producer outer loop... 187 messages)
Precondtion: An S3 bucket named 'dhurtgen-traffic-metrics'
"""

from kafka import KafkaConsumer
import json
import boto3
from botocore.exceptions import ClientError
import logging
from datetime import datetime
import pandas as pd
from traffic-metrics import TrafficMetrics

# create a dynamodb object for traffic metrics
traffic = TrafficMetrics(boto3.resource('dynamodb'))

# create a table of metrics in DynamoDB if it doesn't already exist
if not traffic.exists('Traffic_Metrics'):
    print("\nCreating table Traffic_Metrics...")
    traffic.create_table('Traffic_Metrics')
    print("\nCreated table Traffic_Metrics.")
else:
    print("\nTable Traffic_Metrics already exists.")

# create a client object for s3
s3_client = boto3.client('s3')

# pandas dataframe for json file creation
df = pd.DataFrame(columns=['key', 'timestamp', 'speed', 'num_cars'])

# consumer object
consumer = KafkaConsumer("traffic-simulator")

print("\nListening for messages from Kafka producer, topic traffic-simulator...")

# count for object identification
count = 1
# listen for messages and load them into the DynamoDB table and S3
for msg in consumer:
    message = msg.value.decode('utf-8')
    message_json = json.loads(message)
    key = message_json['key']
    speed = message_json['speed']
    num_cars = message_json['num_cars']
    timestamp = message_json['timestamp']
    # convert timestamp to a datetime object
    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    # convert back to string format for dynamodb
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")
    # load the data into DynamoDB
    traffic.add_traffic_metrics(key, timestamp, speed, num_cars)
    # append to the dataframe
    df.loc[len(df.index)] = [key, timestamp, speed, num_cars]
    # once the dataframe contains a row for each road segment,
    # create a json file of the table and load it into S3
    # as an object
    if len(df) == 187:
        df.to_json('traffic-metrics.json', orient='records')
        object_name = f"tm{count}"
        try:
            s3_client.upload_file('traffic-metrics.json', 'dhurtgen-traffic-metrics', object_name)
        except ClientError as e:
            logging.error(e)
        count += 1
        print(f"\n{len(df)} new lines of data loaded into DynamoDB, table Traffic_Metrics")
        print(f"Object {object_name} uploaded to S3, bucket dhurtgen-traffic-metrics")
        print("\nListening for messages from Kafka producer, topic traffic-simulator...")
        # clear the dataframe for next round of messages
        df = pd.DataFrame(columns=['key', 'timestamp', 'speed', 'num_cars'])
        
# End of program