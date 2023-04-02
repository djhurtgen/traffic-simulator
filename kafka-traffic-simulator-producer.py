#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 12:03:58 2023

@author: vboxuser

Kafka producer for streaming traffic simulation messages
"""

import pandas as pd
from random import normalvariate
import json
from time import sleep
from kafka import KafkaProducer

# import the .csv file
# we don't need the columns spd_limit or congestion
manhattan = pd.read_csv('manhattan.csv', usecols=['street_name', 'from_junc', 'to_junc', 'avg_speed', 'num_cars', 'seg_len'])

# functions for normalizing/denormalizing speed and number of cars
# speeds between 5 and 25 (speed limit)
# num_cars between 0 and 19 for avenues (shorter)
# 0 and 64 for streets (longer)
min_speed = 5
speed_limit = 25
def normalize(x, xmin, xmax):
    return (x - xmin) / (xmax - xmin)

def denormalize(x, xmin, xmax):
    return x * (xmax - xmin) + xmin

# kafka producer for sending messages as json
producer = KafkaProducer(value_serializer = lambda v: json.dumps(v).encode('utf-8'))


for _ in range(40):        
    # wait 30 seconds before each generation cycle     
    sleep(30)
        
    # send a message with first road segment metrics (never changes)
    producer.send("traffic-simulator", value={'key': 0, 'speed': int(manhattan.iat[0, 3]), 'num_cars': int(manhattan.iat[0, 4])})
    
    # create a temporary dataframe for holding updated information...
    manhattan_temp = pd.DataFrame(columns=['speed', 'num_cars'])
    manhattan_temp.loc[len(manhattan_temp.index)] = [int(manhattan.iat[0, 3]), int(manhattan.iat[0, 4])]                               
    
    # set the range of the loop to leave the first and last
    # road segments alone to avoid out of bounds errors
    for i in range(1, len(manhattan) - 1):
        # logic for determining adjacent groups of 3...
        if ((manhattan.iat[i - 1, 1] + 1 == manhattan.iat[i, 1] and manhattan.iat[i - 1, 0] == manhattan.iat[i, 0])
            and (manhattan.iat[i + 1, 1] - 1 == manhattan.iat[i, 1] and manhattan.iat[i + 1, 0] == manhattan.iat[i, 0])) \
            or ((manhattan.iat[i - 1, 1] + 20 == manhattan.iat[i, 1] and manhattan.iat[i - 1, 0] == manhattan.iat[i, 0]) 
                and (manhattan.iat[i + 1, 1] - 20 == manhattan.iat[i, 1] and manhattan.iat[i + 1, 0] == manhattan.iat[i, 0])):
                speed = int(round(normalvariate(
                    mu = (manhattan.iat[i - 1, 3] + manhattan.iat[i, 3] + manhattan.iat[i + 1, 3]) / 3, 
                    sigma = 2
                    )))
                # we don't want speed dropping below 5 mph
                if speed < min_speed:
                    speed = min_speed
                # or being greater than the speed limit
                if speed > speed_limit:
                    speed = speed_limit
                
                speed_normalized = normalize(speed, min_speed, speed_limit)
                num_of_cars_normalized = abs(speed_normalized - 1)
                num_of_cars = int(denormalize(num_of_cars_normalized, 0, (377 * manhattan.iat[i, 5])))
                
                # update temp dataframe
                manhattan_temp.loc[len(manhattan_temp.index)] = [speed, num_of_cars]
                
                # send the message
                producer.send("traffic-simulator", value={'key': i, 'speed': speed, 'num_cars': num_of_cars})
            
        else:
            speed = int(round(normalvariate(mu = manhattan.iat[i, 3], sigma = 2)))
            
            # we don't want speed dropping below 5 mph
            if speed < min_speed:
                speed = min_speed
            # or being greater than the speed limit
            if speed > speed_limit:
                speed = speed_limit
            
            speed_normalized = normalize(speed, min_speed, speed_limit)
            num_of_cars_normalized = abs(speed_normalized - 1)
            num_of_cars = int(denormalize(num_of_cars_normalized, 0, (377 * manhattan.iat[i, 5])))
                
            # update temp dataframe
            manhattan_temp.loc[len(manhattan_temp.index)] = [speed, num_of_cars]
                
            # send the message
            producer.send("traffic-simulator", value={'key': i, 'speed': speed, 'num_cars': num_of_cars})
    
    # send a message with last road segment metrics (never changes)
    producer.send(
        "traffic-simulator", value={'key': len(manhattan) - 1, 'speed': int(manhattan.iat[len(manhattan) - 1, 3]), \
                                    'num_cars': int(manhattan.iat[len(manhattan) - 1, 4])})
    
    # update temp dataframe
    manhattan_temp.loc[len(manhattan_temp.index)] = [int(manhattan.iat[len(manhattan) - 1, 3]), int(manhattan.iat[len(manhattan) - 1, 4])]
    
    # update manhattan
    for i in range(len(manhattan)):
        manhattan.iat[i, 3] = manhattan_temp.iat[i, 0]
        manhattan.iat[i, 4] = manhattan_temp.iat[i, 1]