#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 12:03:58 2023

@author: vboxuser

Kafka consumer for consuming and aggregating traffic simulation messages and
producing .csv files with aggregated results
"""

from kafka import KafkaConsumer
import pandas as pd
import json

# consumer
consumer = KafkaConsumer("traffic-simulator")

# build a pandas dataframe
df = pd.DataFrame(columns=['key', 'speed', 'num_cars'])

# read in manhattan.csv as template for .csv output
manhattan = pd.read_csv('manhattan.csv')

# function for calculating congestion
min_speed = 5
def calcCongestion(nc, ls, v, l):
    non_normalized_congestion = (nc / (377 * ls)) / (v / l)
    normalized_congestion = non_normalized_congestion / (l / min_speed)
    return normalized_congestion

# count for .csv file naming
count = 0    
# insert messages as values
for msg in consumer:
    message = msg.value.decode('utf-8')
    print(message)
    # convert to json
    message_json = json.loads(message)
    key = message_json['key']
    speed = message_json['speed']
    num_cars = message_json['num_cars']
    df.loc[len(df.index)] = [key, speed, num_cars]
    
    # check the length of the dataframe...
    if len(df) == len(manhattan) * 10:
        count += 1
        avg_speeds = df['speed'].groupby(df['key']).mean()
        avg_num_cars = df['num_cars'].groupby(df['key']).mean()
        df_averages = pd.concat([avg_speeds, avg_num_cars], axis=1)
        print(df_averages)
        # populate manhattan with new avg_speed, num_cars, and congestion values and output to .csv
        for i in range(len(manhattan)):
            manhattan.iat[i, 3] = int(df_averages.iat[i, 0])
            manhattan.iat[i, 4] = int(df_averages.iat[i, 1])
            manhattan.iat[i, 7] = round(calcCongestion(manhattan.iat[i, 4], manhattan.iat[i, 5], manhattan.iat[i, 3], manhattan.iat[i, 6]), 4)
            
        # generate the .csv file                
        manhattan.to_csv(f"manhattan_generated{count}.csv", index=False)
        
        # clear out dataframe df
        df = pd.DataFrame(columns=['key', 'speed', 'num_cars'])
