"""
Created on Thu Apr 13 2023

@author: David Hurtgen

Class definition for interacting with AWS DynamoDB
"""

import logging
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class TrafficMetrics:
    """Encapsulates an Amazon DynamoDB table of traffic data."""
    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource
        self.table = None

    def exists(self, table_name):
        """
        Determines whether a table exists. As a side effect, stores the table in
        a member variable.
        :param table_name: The name of the table to check.
        :return: True when the table exists; otherwise, False.
        """
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists

    def create_table(self, table_name):
        """
        Creates an Amazon DynamoDB table that can be used to store traffic data.
        The table uses the road segment key as the partition key and the
        timestamp as the sort key.

        :param table_name: The name of the table to create.
        :return: The newly created table.
        """
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'segment_key', 'KeyType': 'HASH'}, # Partition key
                    {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}   # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'segment_key', 'AttributeType': 'N'},
                    {'AttributeName': 'timestamp', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table

    def add_traffic_metrics(self, key, timestamp, speed, num_cars):
        """
        Adds traffic metrics to the table.
        :param key: Identifies the length of road.
        :param timestamp: Identifies time of recording.
        :param year: Speed of cars.
        :param plot: Number of cars.
        """
        try:
            self.table.put_item(
                Item={
                    'segment_key': key,
                    'timestamp': timestamp,
                    'speed': speed,
                    'num_cars': num_cars})
        except ClientError as err:
            logger.error(
                "Couldn't add data %s to table %s. Here's why: %s: %s", self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def write_batch(self, traffic_metrics):
        """
        Fills an Amazon DynamoDB table with the specified data, using the Boto3
        Table.batch_writer() function to put the items in the table.
        Inside the context manager, Table.batch_writer builds a list of
        requests. On exiting the context manager, Table.batch_writer starts sending
        batches of write requests to Amazon DynamoDB and automatically
        handles chunking, buffering, and retrying.
        :param movies: The data to put in the table. Each item must contain at least
                       the keys required by the schema that was specified when the
                       table was created.
        """
        try:
            with self.table.batch_writer() as writer:
                for traffic_data in traffic_metrics:
                    writer.put_item(Item=traffic_data)
        except ClientError as err:
            logger.error(
                "Couldn't load data into table %s. Here's why: %s: %s", self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    
    def query_traffic_metrics(self, key):
        """
        Queries for traffic metrics for the specified road segment.
        :param key: The road segment key to query.
        :return: The list of road segments relevant to the key.
        """
        try:
            response = self.table.query(KeyConditionExpression=Key('segment_key').eq(key))
        except ClientError as err:
            logger.error(
                "Couldn't query for traffic metrics along segment %s. Here's why: %s: %s", key,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Items']

# End of program